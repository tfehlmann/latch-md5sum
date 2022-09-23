"""
Compute the md5 checksum of a file.
"""

import hashlib
import logging
from typing import cast

from latch import small_task, workflow
from latch.resources.launch_plan import LaunchPlan
from latch.types import (
    LatchAuthor,
    LatchFile,
    LatchMetadata,
    LatchOutputFile,
    LatchParameter,
)

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)

CHUNK_SIZE = 4096
UPDATE_INTERVAL = int(500 * 1024 * 1024 / CHUNK_SIZE)  # all 500 MB


@small_task
def compute_md5sum_task(file: LatchFile, output_file: LatchFile) -> LatchFile:
    """
    Compute the md5sum of a file.
    """
    md5sum = hashlib.md5()
    with open(file, "rb") as f:
        for i, chunk in enumerate(iter(lambda: cast(bytes, f.read(CHUNK_SIZE)), b"")):
            if i > 0 and i % UPDATE_INTERVAL == 0:
                mb_read = i * CHUNK_SIZE / 1024 / 1024
                logging.info("Read %.2f MB", mb_read)
            md5sum.update(chunk)
    result = md5sum.hexdigest()
    with open("/tmp/md5sum.txt", "w", encoding="UTF-8") as f:
        f.write(f"{result}\t{file.remote_path}\n")
    return LatchFile("/tmp/md5sum.txt", remote_path=output_file.remote_path)  # type: ignore


metadata = LatchMetadata(
    display_name="Compute the md5 checksum of a file",
    author=LatchAuthor(
        name="Tobias Fehlmann",
        email="tobias@astera.org",
        github="https://github.com/tfehlmann",
    ),
    repository="https://github.com/tfehlmann/latch-md5sum",
    license="MIT",
    parameters={
        "file": LatchParameter(
            display_name="File",
            description="File for which the md5sum should be computed.",
            batch_table_column=True,
        ),
        "output_file": LatchParameter(
            display_name="Output file",
            description="File in which the md5sum will be stored.",
            batch_table_column=True,
            output=True,
        ),
    },
    tags=[],
)


@workflow(metadata)
def compute_md5sum(file: LatchFile, output_file: LatchOutputFile) -> LatchFile:
    """
    Compute the md5 checksum of a file.
    """
    return compute_md5sum_task(file=file, output_file=output_file)


LaunchPlan(
    compute_md5sum,  # type: ignore
    "Test Data",
    {
        "file": LatchFile("s3://latch-public/init/r1.fastq"),
    },
)

if __name__ == "__main__":
    import os

    project_dir = os.path.dirname(os.path.dirname(__file__))
    compute_md5sum(
        file=LatchFile(os.path.join(project_dir, "data", "test_file.txt")),  # type: ignore
        output_file=LatchOutputFile("/tmp/md5sum.txt"),  # type: ignore
    )  # type: ignore

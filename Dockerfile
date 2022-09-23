FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
COPY wf /root/wf
RUN python3 -m pip install --upgrade latch
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root

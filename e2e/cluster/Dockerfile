# One container that is a whole Slurm cluster, for e2e/workflow.yml:
# slurmctld, slurmd for one node, and sshd for the action to log in to, as
# it would to a login node. There is no slurmdbd, so sacct cannot tell of a
# job, as on a cluster without accounting.
FROM debian:trixie@sha256:3cda3ca472ce45507a5109c6a3907093e24d9ae67d55954c9a07a45665dafba2
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      slurmctld slurmd slurm-client munge openssh-server \
 && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash probe \
 && mkdir -p /run/sshd /run/munge /var/spool/slurmctld /var/spool/slurmd /var/log/slurm \
 && chown slurm: /var/spool/slurmctld /var/log/slurm \
 && chown munge: /run/munge
COPY slurm.conf cgroup.conf /etc/slurm/
COPY entrypoint.sh /entrypoint.sh
EXPOSE 22
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]

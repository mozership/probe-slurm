# One container that is a whole Slurm cluster, for e2e/workflow.yml:
# slurmctld, slurmd for one node, sshd for the action to log in to, as it
# would to a login node, and slurmrestd, which takes the JSON Web Tokens of
# scontrol token. There is no slurmdbd, so neither sacct nor slurmrestd can
# tell of a job slurmctld has forgotten, as on a cluster without accounting.
FROM debian:trixie@sha256:3cda3ca472ce45507a5109c6a3907093e24d9ae67d55954c9a07a45665dafba2
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      slurmctld slurmd slurm-client slurmrestd slurm-wlm-jwt-plugin munge openssh-server \
 && rm -rf /var/lib/apt/lists/*
# slurmrestd runs as a user of its own, as it refuses to run as root or
# SlurmUser. It looks up the name of each client, which takes as long as the
# lookup times out where the host has no name, so names are looked up in
# /etc/hosts alone.
RUN useradd -m -s /bin/bash probe \
 && useradd -r -s /usr/sbin/nologin restd \
 && sed -i 's/^hosts:.*/hosts: files/' /etc/nsswitch.conf \
 && mkdir -p /run/sshd /run/munge /var/spool/slurmctld /var/spool/slurmd /var/log/slurm \
 && chown slurm: /var/spool/slurmctld /var/log/slurm \
 && chown munge: /run/munge
COPY slurm.conf cgroup.conf /etc/slurm/
COPY entrypoint.sh /entrypoint.sh
EXPOSE 22 6820
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]

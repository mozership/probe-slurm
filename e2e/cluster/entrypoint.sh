set -e
# The key the action logs in with, as user probe.
install -d -m 700 -o probe -g probe /home/probe/.ssh
printf '%s\n' "$AUTHORIZED_KEY" > /home/probe/.ssh/authorized_keys
chown probe:probe /home/probe/.ssh/authorized_keys
chmod 600 /home/probe/.ssh/authorized_keys

ssh-keygen -A
[ -f /etc/munge/munge.key ] || mungekey --create
chown munge: /etc/munge/munge.key
chmod 400 /etc/munge/munge.key
# The key slurmctld signs and checks the tokens of scontrol token with.
dd if=/dev/urandom of=/etc/slurm/jwt_hs256.key bs=32 count=1 2>/dev/null
chown slurm: /etc/slurm/jwt_hs256.key
chmod 600 /etc/slurm/jwt_hs256.key
runuser -u munge -- munged
slurmctld
slurmd -N c1
# Only the API of slurmctld, as there is no slurmdbd. A container cannot
# unshare the namespaces slurmrestd would put itself in.
runuser -u restd -- env SLURM_JWT=daemon SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files \
  slurmrestd -a rest_auth/jwt -s openapi/slurmctld 0.0.0.0:6820 &
exec /usr/sbin/sshd -D -e

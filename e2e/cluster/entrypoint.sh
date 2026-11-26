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
runuser -u munge -- munged
slurmctld
slurmd -N c1
exec /usr/sbin/sshd -D -e

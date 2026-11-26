package runner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHConfig says where the login node is and how to log in to it.
type SSHConfig struct {
	Host string
	Port int
	User string
	// KeyFile is the private key to log in with. Without it, the keys of
	// the agent at SSH_AUTH_SOCK are tried, then the default keys under
	// ~/.ssh that have no passphrase.
	KeyFile       string
	KeyPassphrase string
	// KnownHosts is the known_hosts file the host key is checked against.
	// Without it, ~/.ssh/known_hosts and /etc/ssh/ssh_known_hosts are.
	KnownHosts string
	// ClientVersion is what the client calls itself to the server, as
	// SSH-2.0-name_version.
	ClientVersion string
}

// Address is the host and port, as the guard and known_hosts take them.
func (c SSHConfig) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// SSH runs the commands on a login node, each in a session of one
// connection.
type SSH struct {
	client *ssh.Client
}

// systemKnownHosts is the machine-wide known_hosts file, replaced in tests.
var systemKnownHosts = "/etc/ssh/ssh_known_hosts"

// defaultKeys are the keys tried, as OpenSSH tries them, when neither a key
// file nor an agent is given.
var defaultKeys = []string{"id_ed25519", "id_ecdsa", "id_rsa"}

// DialSSH connects and logs in to the login node.
func DialSSH(ctx context.Context, c SSHConfig, getenv func(string) string) (*SSH, error) {
	auth, closeAgent, err := authMethods(c, getenv)
	if err != nil {
		return nil, err
	}
	defer closeAgent()
	hostKey, algorithms, err := hostKeyCallback(c)
	if err != nil {
		return nil, err
	}
	config := &ssh.ClientConfig{
		User:              c.User,
		Auth:              auth,
		HostKeyCallback:   hostKey,
		HostKeyAlgorithms: algorithms,
		ClientVersion:     c.ClientVersion,
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.Address())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", c.Address(), err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	sc, chans, reqs, err := ssh.NewClientConn(conn, c.Address(), config)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to log in to %s as %s: %w", c.Address(), c.User, err)
	}
	// The deadline covered the handshake; each command is bounded by its ctx.
	_ = conn.SetDeadline(time.Time{})
	return &SSH{client: ssh.NewClient(sc, chans, reqs)}, nil
}

func (s *SSH) Run(ctx context.Context, argv []string, stdin []byte) (Output, error) {
	if len(argv) == 0 {
		return Output{}, errors.New("no command to run")
	}
	session, err := s.client.NewSession()
	if err != nil {
		return Output{}, fmt.Errorf("failed to open an SSH session: %w", err)
	}
	defer func() { _ = session.Close() }()
	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if stdin != nil {
		session.Stdin = bytes.NewReader(stdin)
	}
	done := make(chan error, 1)
	go func() { done <- session.Run(Quote(argv)) }()
	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		_ = session.Close()
		return Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}, fmt.Errorf("%s: %w", argv[0], ctx.Err())
	case err = <-done:
	}
	out := Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if exit, ok := errors.AsType[*ssh.ExitError](err); ok {
		out.Code = exit.ExitStatus()
		return out, nil
	}
	if err != nil {
		return out, fmt.Errorf("%s: %w", argv[0], err)
	}
	return out, nil
}

func (s *SSH) Close() error {
	return s.client.Close()
}

func authMethods(c SSHConfig, getenv func(string) string) ([]ssh.AuthMethod, func(), error) {
	noop := func() {}
	if c.KeyFile != "" {
		signer, err := readKey(c.KeyFile, c.KeyPassphrase)
		if err != nil {
			return nil, noop, err
		}
		return []ssh.AuthMethod{ssh.PublicKeys(signer)}, noop, nil
	}
	if sock := getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			// The agent is asked for signatures during the handshake only.
			return []ssh.AuthMethod{ssh.PublicKeysCallback(agent.NewClient(conn).Signers)}, func() { _ = conn.Close() }, nil
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, noop, fmt.Errorf("failed to find the home directory: %w", err)
	}
	var signers []ssh.Signer
	for _, name := range defaultKeys {
		signer, err := readKey(filepath.Join(home, ".ssh", name), "")
		if err == nil {
			signers = append(signers, signer)
		}
	}
	if len(signers) == 0 {
		return nil, noop, errors.New("no key to log in with: give key_file, run an agent at SSH_AUTH_SOCK, or keep a key without a passphrase under ~/.ssh")
	}
	return []ssh.AuthMethod{ssh.PublicKeys(signers...)}, noop, nil
}

func readKey(path, passphrase string) (ssh.Signer, error) {
	path, err := ExpandHome(path)
	if err != nil {
		return nil, err
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the key: %w", err)
	}
	var signer ssh.Signer
	if passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read the key %s: %w", path, err)
	}
	return signer, nil
}

// hostKeyCallback checks the host key against known_hosts, and returns the
// algorithms of the keys it knows for the host, for the server to offer one
// of them rather than another it does not know.
func hostKeyCallback(c SSHConfig) (ssh.HostKeyCallback, []string, error) {
	var files []string
	if c.KnownHosts != "" {
		path, err := ExpandHome(c.KnownHosts)
		if err != nil {
			return nil, nil, err
		}
		files = []string{path}
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find the home directory: %w", err)
		}
		for _, p := range []string{filepath.Join(home, ".ssh", "known_hosts"), systemKnownHosts} {
			if _, err := os.Stat(p); err == nil {
				files = append(files, p)
			} else if !errors.Is(err, fs.ErrNotExist) {
				return nil, nil, fmt.Errorf("failed to read known hosts file: %w", err)
			}
		}
		if len(files) == 0 {
			return nil, nil, errors.New("no known_hosts file to check the host key against: give known_hosts, or add the host to ~/.ssh/known_hosts")
		}
	}
	cb, err := knownhosts.New(files...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read known_hosts: %w", err)
	}
	return cb, knownAlgorithms(cb, c.Address()), nil
}

// knownAlgorithms asks cb about a key no host has, which it answers with the
// keys it knows for address.
func knownAlgorithms(cb ssh.HostKeyCallback, address string) []string {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil
	}
	probe, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil
	}
	var keyErr *knownhosts.KeyError
	if !errors.As(cb(address, &net.TCPAddr{IP: net.IPv4zero}, probe), &keyErr) {
		return nil
	}
	var algorithms []string
	for _, k := range keyErr.Want {
		for _, a := range algorithmsOf(k.Key.Type()) {
			if !slices.Contains(algorithms, a) {
				algorithms = append(algorithms, a)
			}
		}
	}
	return algorithms
}

func algorithmsOf(keyType string) []string {
	switch keyType {
	case ssh.KeyAlgoRSA:
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	case ssh.CertAlgoRSAv01:
		return []string{ssh.CertAlgoRSASHA512v01, ssh.CertAlgoRSASHA256v01, ssh.CertAlgoRSAv01}
	}
	return []string{keyType}
}

// ExpandHome takes a leading ~/ of path as the home directory, as a shell
// does.
func ExpandHome(path string) (string, error) {
	rest, ok := strings.CutPrefix(path, "~/")
	if !ok {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to find the home directory: %w", err)
	}
	return filepath.Join(home, rest), nil
}

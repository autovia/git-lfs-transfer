# git-lfs-transfer

git-lfs-transfer is the server-side implementation of Git LFS using the SSH protocol.

```bash
git-lfs-transfer <git-dir> <operation>
```

Invoked by Git LFS client and updates the repository with the blob files from the remote end.

The command is usually not invoked directly by the end user. The UI for the [GIT LFS SSH protocol](https://github.com/git-lfs/git-lfs/blob/main/docs/proposals/ssh_adapter.md) is on the client side, and the program pair is meant to be used to push/pull updates to remote repository.

## Getting Started

### Installing

#### From release

- Download the latest version from [releases](https://github.com/autovia/git-lfs-transfer/releases)
- On Linux copy the `bin/git-lfs-transfer` binary to `usr/local/bin`

#### From source

- Ensure you have the latest version of Go, SSH server and Git installed.
- Clone the repository
- Run `make build`
- On Linux copy the `bin/git-lfs-transfer` binary to `usr/local/bin`

## License

MIT

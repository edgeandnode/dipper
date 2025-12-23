Contributing to the project
===========================

<!--TODO: ## Code of conduct -->
<!--TODO: ## Issues -->
<!--TODO: ## Pull requests -->
<!--TODO: ## Style guides -->

## Development

### Development prerequisites

Before you can contribute to this project, you need to install and set up the following tools:

- **Rust**: Install Rust through `rustup`, which you can get from the [official Rust website](https://rustup.rs/).
  After installation, you can verify it by running `cargo --version` in your terminal.

- **Just**: Install `just`, a command runner that simplifies project workflows. You can find the installation
  instructions in the [official just documentation](https://github.com/casey/just). After installation, you can verify it by
  running `just --version` in your terminal.

- **Mozilla SOPS**: Install SOPS for managing encrypted environment variables. You can find the installation
  instructions in the [official SOPS documentation](https://github.com/mozilla/sops#readme).
  After installation, you can verify it by running `sops --version` in your terminal.

- **Age**: Age is a simple, modern and secure file encryption tool, used by SOPS for encrypting secrets.
  You can install it from the [official Age GitHub repository](https://github.com/FiloSottile/age). After installation,
  you can verify it by running `age --version` in your terminal.

- **Docker**: Install Docker from the [official Docker website](https://docs.docker.com/get-docker/). 
  After installation, you can verify it by running `docker --version` in your terminal.

Please ensure you have all these prerequisites in place before you start contributing to the project.

> [!TIP]
> You can run `just --list` to see all available development tasks.

<!-- TODO: ### Project structure -->


### Rust workflow

#### Formatting

Code formatting is automatically enforced using `rustfmt`.

> [!WARNING]
> Automatic imports formatting require the use of `nightly` Rust toolchain.

```shell
just fmt
```

To check Rust code formatting without making changes, you can use:

```shell
just fmt-check
```

#### Linting

Code linting is automatically enforced using `clippy`. 

In the CI pipeline, we use the following command to run the linter:

```shell
just check
```

#### Testing

> [!TIP]
> Tests that require access to secrets should be run using the encrypted `.env` file. 
> Refer to the [CI Secrets](#running-the-tests) section for more information.

To run the project **unit tests**, you can use the following command:

```shell
just test-unit
```

To run the project **integration tests**, you can use the following command:

```shell
just test-it
```

### Git hooks

This project uses [pre-commit](https://pre-commit.com/) to automatically run code formatting checks before commits are made. Git hooks help ensure code quality and consistency by automatically running the formatting tools whenever you commit changes.

> [!NOTE]
> Git hooks are **optional** and **opt-in**. You can contribute to the project without installing them, but they provide a convenient way to automatically format your code before commits.

#### Prerequisites

Before setting up Git hooks, you need to install `pre-commit`. You can install it using your preferred package manager:

```shell
# Using pip
pip install pre-commit

# Using pacman (Arch Linux)
pacman -S pre-commit

# Using apt (Ubuntu/Debian)
apt-get install pre-commit

# Using Homebrew (macOS)
brew install pre-commit
```

#### Installing Git hooks

To install the pre-commit hooks for this project, run:

```shell
just install-git-hooks
```

This command will:
1. Check if `pre-commit` is installed on your system
2. Install the pre-commit hooks defined in [`.github/pre-commit-config.yaml`](.github/pre-commit-config.yaml)

#### Removing Git hooks

If you need to remove the Git hooks, you can run:

```shell
just remove-git-hooks
```

#### Running hooks manually

You can also run the pre-commit hooks manually without making a commit:

```shell
# Run all hooks on all files
pre-commit run --all-files --config .github/pre-commit-config.yaml

# Run specific hook
pre-commit run format --config .github/pre-commit-config.yaml
```

> [!TIP]
> If you need to commit changes without running the hooks (not recommended), you can use `git commit --no-verify`.

### CI Secrets

In this project, we handle secrets through an encrypted `.env` file. This file holds various environment variables,
encrypted using [Mozilla SOPS](https://github.com/mozilla/sops), a secure tool for managing and storing secrets. We
utilize [Age](https://github.com/FiloSottile/age), a simple, modern, and secure file encryption tool, as the encryption
backend for SOPS. This method allows us to manage secrets without the need for contributors to access GitHub Actions
secrets, offering a secure way to introduce new test credentials required by the tests.

#### Prerequisites

1. **Install Age and SOPS**: Ensure you have both Age and SOPS installed on your machine. For more information refer to
   the [Prerequisites](#prerequisites) section.
2. **Generate a new key pair**: Generate a new key pair using the following command:

   ```shell
   age-keygen -o keys.txt
   ```

   This command generates a new key pair and saves it to the `keys.txt` file. This key pair is used to encrypt and
   decrypt the `.env` file.

   According to the [SOPS documentation](https://github.com/getsops/sops#encrypting-using-age), by default SOPS will
   look for a `<user-config-dir>/sops/age/keys.txt` file under the user's configuration directory
   (e.g., `$XDG_CONFIG_HOME/sops/age/keys.txt` in Linux).

#### Running the tests

To run the tests, you need to decrypt the `.env` file using the `sops exec-env` command. This command decrypts the
`.env` file and sets the environment variables for the command that follows it.

For example, to **_execute the project integration tests_** with the decrypted environment variables, you can use the
following command:

```shell
sops exec-env .env "just test-it"
```

The command above assumes that a valid `keys.txt` file is present in the user's configuration directory.

Alternatively, if you have a different key file or location, you can specify it using the `SOPS_AGE_KEY_FILE`
environment variable or passing the Age private key content directly to the `sops exec-env` command via
the `SOPS_AGE_KEY` environment variable.

```shell
SOPS_AGE_KEY_FILE=/path/to/keys.txt sops exec-env .env "just test-it"
```

#### Adding new contributors public keys

In order to any new contributor to be able to decrypt the `.env` file, the file needs to be encrypted using their public
key. To do so, we need to add the new contributor's public key as recipients to the `.env` file. To do so, you have to
ask a project maintainer to encrypt the `.env` file using the new contributor's public key. To do so, run the following
command:

```shell
sops --rotate --add-age <new-recipient-age-key> --in-place .env
```

The command above assumes that the `.env` file is already encrypted with the public keys of the project contributors,
and the project maintainer has the private keys to decrypt the file.

#### Adding new secrets

Once your Age public key is associated with the `.env` file, you can add new secrets to the file. To do so, you can use
the following command to add a new secret to the file:

```shell
sops --set '["<env-var-name>"] "<env-var-value>"' .env
```

Note that the `env-var-name` and `env-var-value` should be replaced with the name and value of the new environment
variable you want to add to the `.env` file. If an environment variable with the same name already exists, the command
will update the value of the existing environment variable.

#### Encrypting and decrypting the `.env` file

> [!WARNING]
>
> This procedure is completely discouraged due to the ⚠️**risk of leaking sensitive information**⚠️. Only project
> maintainers
> should encrypt and decrypt the `.env` file.
>
> Refer to the [Adding new contributors public keys](#adding-new-contributors-public-keys)
> or [Adding new secrets](#adding-new-secrets) sections for a safer way to manipulate the `.env` file.

To encrypt the `.env` file in-place, you can use the following command where `<recipien-age-key>` are the public keys
of the different project contributors:

```shell
sops --encrypt --age <recipient-age-key>[,<recipient-age-key>,...] .env
```

Assuming that the `.env` file is encrypted with the public keys of the project contributors, and your private key is
present in the `keys.txt` file, you can decrypt the `.env` file using the following command:

```shell
sops --decrypt .env
```

Both commands will print the decrypted content to the standard output. If you want to save the decrypted content to a
file, you can redirect the output to a file, or use the `--in-place` flag to overwrite the file in-place.

<!--TODO: ## Glossary -->

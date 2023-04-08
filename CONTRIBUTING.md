# Contributing to Ez-transform (Ezt)

Thanks for taking the time to contribute! We appreciate all contributions, from reporting bugs to implementing new features.

## Table of contents

- [Contributing to Ez-transform (Ezt)](#contributing-to-ez-transform-ezt)
  - [Table of contents](#table-of-contents)
  - [Reporting bugs](#reporting-bugs)
  - [Suggesting enhancements](#suggesting-enhancements)
  - [Contributing to the codebase](#contributing-to-the-codebase)
    - [Picking an issue](#picking-an-issue)
    - [Setting up your local development environment](#setting-up-your-local-development-environment)
    - [Working on your issue](#working-on-your-issue)
    - [Pull requests](#pull-requests)
  - [Contributing to documentation](#contributing-to-documentation)
  - [License](#license)

## Reporting bugs

We use [GitHub issues](https://github.com/ez-transform/ezt-core/issues) to track bugs and suggested enhancements.
You can report a bug by opening a [new issue](https://github.com/ez-transform/ezt-core/issues/new/choose).

Before creating a bug report, please check that your bug has not already been reported, and that your bug exists on the latest version of Ezt.
If you find a closed issue that seems to report the same bug you're experiencing, open a new issue and include a link to the original issue in your issue description.

Please include as many details as possible in your bug report. The information helps the maintainers resolve the issue faster.

## Suggesting enhancements

We use [GitHub issues](https://github.com/ez-transform/ezt-core/issues) to track bugs and suggested enhancements.
You can suggest an enhancement by opening a [new feature request](https://github.com/ez-transform/ezt-core/issues/new/choose).
Before creating an enhancement suggestion, please check that a similar issue does not already exist.

Please describe the behavior you want and why, and provide examples of how Ezt would be used if your feature were added.

## Contributing to the codebase

### Picking an issue

Pick an issue by going through the [issue tracker](https://github.com/ez-transform/ezt-core/issues) and find an issue you would like to work on. Feel free to pick any issue that is not already assigned.

If you would like to take on an issue, please comment on the issue to let others know. You may use the issue to discuss possible solutions.

### Setting up your local development environment

Ezt is developed using Python with Poetry as the packaging and dependency manager.

To contribute to Ezt, make sure you are using one of the currently supported Python versions (see pyproject.toml in project root folder). If you need to install a new Python version, you can use for example [pyenv](https://github.com/pyenv/pyenv#installation).

Your development environment should also have Poetry installed. If Poetry is not familiar to you, please check out [Poetry docs](https://python-poetry.org/docs/) on how to install and use it. Older versions of Poetry (<1.0.0) might be problematic and may not support all features used for packaging and dependency management in Ezt.


_Note that if you are a Windows user, the steps below might not work as expected; try developing using [WSL](https://learn.microsoft.com/en-us/windows/wsl/install)._

Start by [forking](https://docs.github.com/en/get-started/quickstart/fork-a-repo) the ezt-core repository, then clone your forked repository using `git`:

```bash
git clone https://github.com/ez-transform/ezt-core.git
cd ezt-core
```

Make sure you deactivate any active virtual environments or conda environments, as poetry will automatically create a virtual environment for you.

Finally, run `poetry install` from the base of the cloned repository, where your `pyproject.toml` is located. This will create a virtual environment and install all dependencies as well as Ezt itself.

If the installation was successful, you should be able to activate the virtual environment created by poetry, and running `ezt` from the command line. If you see an output saying something like `Welcome to Ezt!`, then you're ready to start contributing to the Ezt codebase!

### Working on your issue

Create a new git branch from the `main` branch in your local repository, and start coding!

Two other things to keep in mind:

- If you add code that should be tested, add tests.
- If you change any features that impact how the users interact with the tool, [update the documentation](#contributing-to-documentation).

### Pull requests

When you have resolved your issue, [open a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) in the Ezt repository.
Please adhere to the following guidelines:

- Use a descriptive title. This text will end up in the [changelog](https://github.com/ez-transform/ezt-core/releases).
- In the pull request description, [link](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue) to the issue you were working on.
- Add any relevant information to the description that you think may help the maintainers review your code.
- Make sure your branch is [rebased](https://docs.github.com/en/get-started/using-git/about-git-rebase) against the latest version of the `main` branch.

After you have opened your pull request, a maintainer will review it and possibly leave some comments. Once all issues are resolved, the maintainer will merge your pull request, and your work will be part of the next Ezt release!

Keep in mind that your work does not have to be perfect right away! If you are stuck or unsure about your solution, feel free to open a draft pull request and ask for help.

## Contributing to documentation

The Ezt documentation is located in the same repository in the `docs` folder. The documentation is built with MkDocs, so all documentation pages are written using markdown. If your contribution impacts anything that the users of Ezt should be aware of, please update the documentation accordingly. Please have a look at the [documentation](https://ez-transform.github.io/ezt-core/) to get an overview of it.

Ezt uses MkDocs to build the documentation, which means that you can locally test your contribution by starting the mkdocs development server with `mkdocs serve`, and the navigate to http://127.0.0.1:8000 in your browser. This will also tell you if there is an issue with your addition to the docs.

## License

Any contributions you make to this project will fall under the [Apache 2.0 License](LICENSE) that covers the Ezt project.

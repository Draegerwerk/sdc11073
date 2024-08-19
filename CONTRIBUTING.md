# How to contribute to Drägerwerk sdc11073

Welcome to our project. We are glad you want to improve this open source library. We at Dräger want to create technology
for life and help to create a better future 🚀.

If you provide Markdown documents, issues, pull request or discussions we strongly encourage you
to [format your text](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax)
as it greatly helps to read and to understand your provided information.

For this open source project
the [Contributor License Agreement](https://github.com/Draegerwerk/sdc11073/blob/master/Contributor_License_Agreement.md)
governs
all relevant activities and your contributions. By contributing to the project you agree to be bound by this Agreement
and to license your work accordingly.

## How to set up the project for development

- As we restricted pushing directly to one of sdc11073's branches, you have
  to [create a fork](https://github.com/Draegerwerk/sdc11073/fork) where you can push your changes before creating a
  pull request.
- Install the package as an editable installation with `pip install -e ".[dev]"`.
- Before making any commits, ensure
  you [sign your commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits).

## Get support

By following these guidelines, you help us to address your concerns more efficiently.

### Create an issue

If you encounter any problems or have suggestions for improvements, we encourage you to create
an [issue](https://github.com/Draegerwerk/sdc11073/issues/new/choose).

Here's how you can do it effectively:

1. **Search Existing Issues**: Before creating a new issue, please search the existing issues to avoid duplicates. If
   you find an issue that addresses your problem or suggestion, feel free to add a comment to it.

2. **Choose the Right Template**: When creating a new issue, select the template that best matches your situation. We
   have templates for bug reports and feature requests.

3. **Provide Detailed Information**: Give a concise and informative title and fill out the rest of the form to your best
   knowledge. The more information you provide the better we can help you.

4. **Submit the Issue**: Once you've filled out all the necessary information, submit the issue.

### Create a discussion

Engaging in discussions is a great way to ask questions, share ideas, or seek help with setting up `sdc11073`. Here's a
step-by-step guide on how to create a discussion:

1. **Navigate to the Discussions Tab**: Go to the main page of the `sdc11073` repository on GitHub. Click on the "
   Discussions" tab near the top of the page or
   follow [this link](https://github.com/Draegerwerk/sdc11073/discussions/new/choose) directly.

2. **Choose a Category**: Select the appropriate category for your discussion. Categories might include Q&A, Ideas,
   General, etc., depending on what you want to discuss.

3. **Title Your Discussion**: Enter a concise yet descriptive title for your discussion. This helps others understand at
   a glance what your discussion is about.

4. **Write Your Message**: In the message body, provide detailed information about your question, idea, or the help
   you're seeking. Formatting tools and Markdown are available to structure your text.

5. **Tag Your Discussion (Optional)**: You can add relevant labels to your discussion to make it easier for others to
   find and to categorize it better.

6. **Review and Post**: Before posting, review your discussion to ensure it's clear and contains all necessary
   information. Once ready, click the "Post" button.

By creating a discussion, you're contributing to the community around `sdc11073`. Engage respectfully and constructively
to make the most out of the discussions.

## Coding Standards / Style Guide

This section outlines the coding standards and style guidelines for our project. Adhering to these guidelines ensures
code readability, maintainability, and consistency across the project. It's crucial for all contributors to follow these
practices to facilitate collaboration and code quality.

### General Principles

- **Readability**: Code should be written to be readable by humans. Clarity is preferred over cleverness.
- **Consistency**: Follow the established patterns and practices in the project.
- **Simplicity**: Strive for simplicity in your code. Avoid unnecessary complexity.

### Formatting / Static Code Analysis

- Use [ruff](https://docs.astral.sh/ruff/) to ensure your code adheres to our coding standards.
- Use [mypy](http://mypy-lang.org/) for type checking. Ensure your code is typed and passes mypy checks.

Checkout our [`pyproject.toml`](https://github.com/Draegerwerk/sdc11073/blob/master/pyproject.toml) for more details.

### Naming Conventions

- **Modules**: Use short, lowercase names. If necessary, use underscores to improve readability (e.g., `my_module`).
- **Classes**: Use the CapWords convention (e.g., `MyClass`).
- **Functions and Variables**: Use lowercase with words separated by underscores (e.g., `my_function`).
- **Constants**: Use all uppercase with words separated by underscores (e.g., `MY_CONSTANT`).

### Comments

- Use inline comments sparingly and ensure they are relevant and add value.
- Write docstrings for all public modules, functions, classes, and methods. Follow
  the [reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html) format.

## Testing / Coverage

We take testing seriously to ensure the reliability and stability of our project. Here's how you can contribute to
testing:

### Running Tests

- To run the existing test suite, execute `pytest` at the root of the project. This will run all tests and display a
  report.
- Ensure that all tests pass before submitting a pull request.

### Writing Tests

- When adding new features or fixing bugs, write tests that cover your changes. We strive for comprehensive test
  coverage to maintain code quality.
- Follow our project's conventions for test structure and naming. Tests should be placed in the `tests` directory.
- Use descriptive test function names that clearly state what is being tested.

### Test Coverage

- We aim for high test coverage but understand it's not always practical to achieve 100%. Focus on testing critical
  paths and complex logic.
- After running tests, check the coverage report to ensure your changes are adequately covered. You can generate a
  coverage report by running `pytest --cov=src`.
- We use [codecov](https://app.codecov.io/gh/Draegerwerk/sdc11073) to monitor test coverage. Ensure your pull request
  does not significantly decrease the project's overall coverage.

By following these guidelines, you help ensure that our project remains stable, reliable, and easy to maintain. Thank
you for contributing to our tests!

## How to create a pull request

Creating a pull request is a critical step in contributing to our project. Here's how to do it effectively:

1. **Ensure Your Branch is Up-to-Date**: Before starting, make sure your branch is up-to-date with the branch you're
   planning to merge into.

2. **Check Your Changes**:
    - Run tests with `pytest` to ensure all tests pass.
    - Use `ruff check /path/to/your/changes1 /path/to/your/changes2 ...` for static code analysis.
    - Apply `mypy /path/to/your/changes1 /path/to/your/changes2 ...` for type checking.

3. **Commit Your Changes**: Commit your changes with clear, descriptive commit messages.

4. **Push to Your Fork**: Push your changes to your fork of the repository.

5. **Create the Pull Request**:
    - Navigate to the original repository you forked from.
    - Click on the "Pull requests" tab and then the "New pull request" button.
    - Choose your fork and the branch with your changes as the "compare" branch and the branch of our repository you
      want to merge into as the "base" branch.
    - Fill in the pull request form with a clear title and a detailed description of your changes.
    - If your pull request is related to an issue or discussion thread, reference it in the description or/and under
      the "Development" section.

6. **Review and Adjust**:
    - After submitting, at least one of our maintainers has to review your pull request. Be open to feedback also from
      other contributors and ready to make adjustments as needed.
    - If requested, make further commits to your branch to address feedback.

7. **Acceptance and Merge**:
    - Once your pull request is approved by a project maintainer and all CI checks pass, it will be merged.
    - Congratulations! You've successfully contributed to the project.

## Acknowledgment

We thank the following contributors for their valuable contributions to the project:

- 2020-2024 Bernd Deichmann
- 2023-2024 Leon Budnick

If you want to be listed as a contributor, add your information in the following format:

```markdown
- <year(s) of contribution(s)> <your name> <optionally your email>
```

If you have contributed in multiple years, you can define a range of years like `2020-2024` or single years
like `2020-2022,2024`. Remember to update them with your first contribution in that year.

**Thank you for contributing to our project!**

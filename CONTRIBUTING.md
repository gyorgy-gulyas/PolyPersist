# Contributing to DDD Interpreter (D3I)

Thank you for your interest in contributing to the DDD Interpreter (D3I) project! Your support and expertise are invaluable for improving this tool. The following guidelines will help ensure that your contributions are well-integrated into the project.

## How to Contribute

1. **Reporting Bugs**: If you encounter any bugs, please open an issue with a clear description of the problem, steps to reproduce, and any error messages you encountered.
2. **Suggesting Enhancements**: We welcome new feature suggestions! Open an issue labeled as a "Feature Request" and describe the enhancement or idea in detail.
3. **Submitting Code Changes**: You can contribute to the codebase by fixing bugs, adding features, or improving documentation. Please ensure your changes adhere to the project’s guidelines.

## Development Setup

Follow these steps to set up a local development environment:

1. **Clone the Repository**:
    ```bash
    git clone https://github.com/gyorgy-gulyas/d3i.git
    cd d3i
    ```

2. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Run Tests**:
    Ensure that existing tests pass before making new changes:
    ```bash
    python -m unittest discover
    ```
3. **Use Visual Studio Code**:

    ```bash
    code .
    ```


## Contribution Workflow

1. **Fork the Repository**: Create a personal fork of the repository on GitHub.
2. **Create a Branch**: Name the branch according to the feature or fix (e.g., `feature/add-java-support` or `fix/event-parsing`).
3. **Make Your Changes**: Implement your changes on the branch, ensuring that code is clean and well-documented.
4. **Run Tests**: Run the all the tests to confirm that everything works as expected.
5. **Commit and Push**: Commit your changes with clear and concise messages. Push the branch to your fork.
6. **Open a Pull Request**: Go to the main repository and open a pull request, describing the changes made.

## Code Style

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines for Python code.
- Keep your commits focused and atomic.
- Update documentation as needed to reflect your changes.

## Community and Support

If you have any questions, feel free to reach out via the issues section or discuss on our GitHub Discussions page.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project. see content of the LICENSE file

---

Thank you for helping to improve DDD Interpreter! We look forward to your contributions.

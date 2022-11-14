import nox


@nox.session(python=["3.7", "3.8", "3.9", "3.10"])
def test(session):
    """Run unit tests with pytest."""
    session.run_always("poetry", "install", external=True)
    session.run("pytest")


@nox.session(python="3.10")
def docs(session):
    """Build docs."""
    session.run_always("poetry", "install", "--only", "docs", external=True)
    session.run("mkdocs", "build")

import nox


@nox.session(python=["3.8", "3.9", "3.10"])
def test(session):
    """Run unit tests with pytest."""
    session.run_always("poetry", "install", "--without", "docs", external=True)
    session.run("pytest")


@nox.session(python="3.10")
def docs(session):
    """Build docs."""
    session.run_always("poetry", "install", "--without", "dev", external=True)
    session.run("mkdocs", "build")

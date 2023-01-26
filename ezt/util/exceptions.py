class EztException(Exception):
    """Base class for exceptions specific to ezt."""

    pass


class ConfigInitException(EztException):
    """
    Exception representing running an ezt-command from a folder not containing
    an ezt_project.yml file.
    """

    def __init__(self, message="Config file ezt_project.yml not found."):
        self.message = message
        super().__init__(self.message)


class PyModelException(EztException):
    """
    Exception representing an invalid df model.
    Raised when incorrect return type is returned from
    user-specified model.
    """

    def __init__(self, message="Incorrect return type in model."):
        self.message = message
        super().__init__(self.message)


class GetSourceException(EztException):
    """
    Raised when trying to get a source that does not exist.
    """

    def __init__(self, message="Source not found."):
        self.message = message
        super().__init__(self.message)


class GetModelException(EztException):
    """
    Raised when trying to get a model that does not exist.
    """

    def __init__(self, message="Model not found."):
        self.message = message
        super().__init__(self.message)


class EztConfigException(EztException):
    """
    Raised when invalid value is used in a yml config file.
    """

    def __init__(self, message="Invalid or missing yml key."):
        self.message = message
        super().__init__(self.message)


class EztInputException(EztException):
    """
    Raised when an invalid input is entered for example when providing an option.
    E.g. 'ezt run --model-name <model_that_dont_exist>'
    """

    def __init__(self, message="Invalid input entered.") -> None:
        self.message = message
        super().__init__(self.message)


class EztMergeException(EztException):
    """
    Raised when something related to merge is failing.
    """

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class EztAuthenticationException(EztException):
    """
    Raised when Ezt notices that authentication credentials are missing.
    """

    def __init__(self, message="Missing authentication credentials.") -> None:
        self.message = message
        super().__init__(self.message)

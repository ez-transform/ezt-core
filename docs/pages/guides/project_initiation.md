# Project initiation

When using Ezt you will be editing files locally using a code editor, and running projects using the terminal on your local machine.

## Prerequisites

In order to use Ezt, we recommend that the user understands the following concepts:

* Basic understanding of how to use the Terminal.
* Basic Python programming language understanding.
* Ezt encourages users to use [Polars](https://pypi.org/project/polars/) as the main data manipulation module for your data models, so
    basic understanding of Polars is needed to build a data model. Polars syntax is not very different compared to Pandas or Spark, but some differences exist. Fortunately Polars has excellent API-documentation at <https://pola-rs.github.io/polars/py-polars/html/reference/> as well as a great book documentation at <https://pola-rs.github.io/polars-book/user-guide/index.html>.

The recommended way to create a new Ezt project is the following:

**1.** From your terminal, create a python virtual environment.

**2.** Activate your virtual environment and install Ezt with the following command:

    pip install ez-transform

**3.** Check that Ezt is accessible from your terminal simply by running the command 'ezt'.

> You should see a "Welcome to Ezt!" -message and some command suggestions. If you see a message indicating that the 'ezt'-command cannot be found, you will need to add it to your Path environment variable.

**4.** Change directory to the place where you want your local directory stored. E.g.:

    cd /home/NinjaTurtle/EztProjects

**5.** Initiate a new Ezt project by writing 'ezt init *project-name*' and replace *project-name* with the name of your choice, like this:

    ezt init myproject

**6.** The previous command will create a new folder **myproject** with a baseline Ezt project structure inside the **myproject** folder. Now, cd into your project folder just created.

**7.** When inside your project folder, run the following command to ensure that everything is in order inside your project:

    ezt check        

> This command will validate all of the configuration files you have inside your project and return an error message if something is not right. Since you haven't done any changes to your project yet, this command should pass with some thumbs up emojis :thumbsup: and messages indicating that some yml-files have been successfully validated.

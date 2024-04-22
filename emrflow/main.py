"""Main entry point for the emrflow CLI!"""

import typer

from emrflow import emr_serverless

app = typer.Typer(pretty_exceptions_show_locals=False)
app.add_typer(emr_serverless.app, name="serverless", help="Utility for EMR Serverless")


if __name__ == "__main__":
    app()

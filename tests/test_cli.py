from typer.testing import CliRunner
from stl.cli import app

runner = CliRunner()


def test_reconcile_command_reports_the_flip():
    res = runner.invoke(app, ["reconcile"])
    assert res.exit_code == 0
    assert "Cy" in res.stdout and "Bo" in res.stdout
    assert "matches oracle" in res.stdout.lower()


def test_checkpoint_restore_reports_exactly_once():
    res = runner.invoke(app, ["checkpoint-restore", "--crash-at", "3"])
    assert res.exit_code == 0
    assert "exactly-once" in res.stdout.lower()


def test_version():
    res = runner.invoke(app, ["version"])
    assert res.exit_code == 0
    assert "0.1.0" in res.stdout

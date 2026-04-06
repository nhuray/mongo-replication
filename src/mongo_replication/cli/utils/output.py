"""
Utilities for colored console output using rich.
"""

from rich.console import Console
from rich.panel import Panel

console = Console()


def print_banner(title: str, **info: str) -> None:
    """
    Print a formatted banner with title and key-value pairs.

    Args:
        title: Banner title
        **info: Key-value pairs to display
    """
    console.print()
    console.rule(f"[bold blue]{title}[/bold blue]", style="blue")

    if info:
        for key, value in info.items():
            console.print(f"  [cyan]{key}:[/cyan] {value}")

    console.print()


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]✓[/green] {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[red]✗[/red] {message}", style="red")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]⚠[/yellow] {message}", style="yellow")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]ℹ[/blue] {message}")


def print_step(step_num: int, total_steps: int, title: str) -> None:
    """
    Print a step header.

    Args:
        step_num: Current step number
        total_steps: Total number of steps
        title: Step title
    """
    console.print()
    console.rule(f"[bold]Step {step_num}/{total_steps}: {title}[/bold]", style="cyan")
    console.print()


def print_summary(title: str, items: dict[str, str | int]) -> None:
    """
    Print a summary panel with key-value pairs.

    Args:
        title: Summary title
        items: Dictionary of items to display
    """
    lines = []
    for key, value in items.items():
        lines.append(f"[cyan]{key}:[/cyan] {value}")

    panel = Panel(
        "\n".join(lines),
        title=f"[bold]{title}[/bold]",
        border_style="green",
    )
    console.print(panel)

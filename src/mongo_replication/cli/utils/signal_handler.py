"""Signal handler utilities for graceful shutdown.

Provides utilities to handle SIGINT (Ctrl+C), SIGTERM, and other signals
gracefully in CLI commands.
"""

import signal
import sys
from typing import Callable, Optional

from rich.console import Console

console = Console()


class SignalHandler:
    """Context manager for handling signals gracefully.

    Usage:
        with SignalHandler() as handler:
            # Your code here
            # Will be interrupted gracefully on Ctrl+C
            pass

        if handler.interrupted:
            # Handle interruption
            pass
    """

    def __init__(
        self,
        cleanup_callback: Optional[Callable] = None,
        message: str = "Operation interrupted by user",
    ):
        """Initialize signal handler.

        Args:
            cleanup_callback: Optional function to call on signal
            message: Message to display on interruption
        """
        self.cleanup_callback = cleanup_callback
        self.message = message
        self.interrupted = False
        self.original_sigint = None
        self.original_sigterm = None

    def __enter__(self):
        """Set up signal handlers."""
        # Store original handlers
        self.original_sigint = signal.signal(signal.SIGINT, self._handle_signal)
        self.original_sigterm = signal.signal(signal.SIGTERM, self._handle_signal)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore original signal handlers."""
        # Restore original handlers
        if self.original_sigint is not None:
            signal.signal(signal.SIGINT, self.original_sigint)
        if self.original_sigterm is not None:
            signal.signal(signal.SIGTERM, self.original_sigterm)

        # Don't suppress exceptions unless it's KeyboardInterrupt
        return exc_type is KeyboardInterrupt

    def _handle_signal(self, signum, frame):
        """Handle signal by setting interrupted flag and calling cleanup."""
        self.interrupted = True

        # Print newline for cleaner output after Ctrl+C
        console.print()
        console.print(f"[yellow]⚠ {self.message}[/yellow]")

        # Call cleanup callback if provided
        if self.cleanup_callback:
            try:
                self.cleanup_callback()
            except Exception as e:
                console.print(f"[red]Error during cleanup: {e}[/red]")

        # Exit gracefully
        sys.exit(130)  # Standard exit code for SIGINT


def setup_signal_handlers(cleanup_callback: Optional[Callable] = None):
    """Setup global signal handlers for the application.

    This is a simpler alternative to the context manager that sets up
    handlers for the entire process lifetime.

    Args:
        cleanup_callback: Optional function to call on signal

    Example:
        setup_signal_handlers(cleanup_callback=lambda: print("Cleaning up..."))
    """

    def signal_handler(signum, frame):
        console.print()
        console.print("[yellow]⚠ Operation interrupted by user[/yellow]")

        if cleanup_callback:
            try:
                cleanup_callback()
            except Exception as e:
                console.print(f"[red]Error during cleanup: {e}[/red]")

        sys.exit(130)

    # Register handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

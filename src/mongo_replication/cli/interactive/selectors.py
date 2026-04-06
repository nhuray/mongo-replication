"""
Interactive collection selection using questionary.
"""

from typing import List, Optional
import questionary
from questionary import Style


# Custom style for questionary
custom_style = Style(
    [
        ("qmark", "fg:#673ab7 bold"),  # Question mark
        ("question", "bold"),  # Question text
        ("answer", "fg:#2196f3 bold"),  # Selected answer
        ("pointer", "fg:#673ab7 bold"),  # Selection pointer
        ("highlighted", "fg:#673ab7 bold"),  # Highlighted choice
        ("selected", "fg:#2196f3"),  # Selected choices (checkbox)
        ("separator", "fg:#cc5454"),  # Separator
        ("instruction", ""),  # Instruction text
        ("text", ""),  # Plain text
        ("disabled", "fg:#858585 italic"),  # Disabled choices
    ]
)


def select_collections(
    available_collections: List[str],
    default_selected: Optional[List[str]] = None,
) -> List[str]:
    """
    Interactively select collections using checkboxes.

    Args:
        available_collections: List of available collection names
        default_selected: Collections to pre-select (defaults to all)

    Returns:
        List of selected collection names
    """
    if not available_collections:
        return []

    # Default to all collections if not specified
    if default_selected is None:
        default_selected = available_collections

    # Create choices with pre-selection
    choices = [
        questionary.Choice(title=col, checked=(col in default_selected))
        for col in sorted(available_collections)
    ]

    selected = questionary.checkbox(
        "Select collections to include:",
        choices=choices,
        style=custom_style,
        instruction="(Space to select/deselect, Enter to confirm)",
    ).ask()

    # Handle cancellation (Ctrl+C)
    if selected is None:
        return []

    return selected


def confirm_action(message: str, default: bool = True) -> bool:
    """
    Ask for confirmation.

    Args:
        message: Confirmation message
        default: Default answer

    Returns:
        True if confirmed, False otherwise
    """
    result = questionary.confirm(
        message,
        default=default,
        style=custom_style,
    ).ask()

    # Handle cancellation (Ctrl+C)
    if result is None:
        return False

    return result


def select_single(
    message: str,
    choices: List[str],
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Select a single option from a list.

    Args:
        message: Selection prompt
        choices: List of available choices
        default: Default choice

    Returns:
        Selected choice or None if cancelled
    """
    return questionary.select(
        message,
        choices=choices,
        default=default,
        style=custom_style,
    ).ask()

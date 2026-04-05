"""
Progress bar utilities using tqdm.
"""

from typing import Iterable, Optional
from tqdm import tqdm


def create_progress_bar(
    iterable: Optional[Iterable] = None,
    total: Optional[int] = None,
    desc: str = "",
    unit: str = "it",
    **kwargs,
) -> tqdm:
    """
    Create a styled tqdm progress bar.
    
    Args:
        iterable: Iterable to wrap (optional)
        total: Total number of iterations (optional)
        desc: Description to display
        unit: Unit name for iterations
        **kwargs: Additional tqdm arguments
    
    Returns:
        tqdm progress bar instance
    """
    default_kwargs = {
        "bar_format": "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        "colour": "cyan",
        "ncols": 80,
    }
    default_kwargs.update(kwargs)
    
    return tqdm(
        iterable=iterable,
        total=total,
        desc=desc,
        unit=unit,
        **default_kwargs,
    )


def progress_wrapper(items: list, desc: str, unit: str = "item") -> Iterable:
    """
    Wrap a list with a progress bar.
    
    Args:
        items: List of items to process
        desc: Description for the progress bar
        unit: Unit name for items
    
    Returns:
        Iterable with progress bar
    """
    return create_progress_bar(
        iterable=items,
        desc=desc,
        unit=unit,
    )

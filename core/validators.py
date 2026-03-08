import re

from core.exceptions import ValidationError


def validate_cql_identifier(name: str, label: str = "identifier") -> str:
    """Validate that a string is safe to use as a CQL identifier (keyspace/table name).

    Allows alphanumeric characters, underscores, and hyphens only.
    Raises ValidationError if the name contains unsafe characters.
    Returns the validated name unchanged.
    """
    if not name or not name.strip():
        raise ValidationError(detail=f"Empty {label} name is not allowed")
    if not re.fullmatch(r"[a-zA-Z0-9_\-]+", name):
        raise ValidationError(
            detail=(
                f"Invalid {label} name '{name}': only alphanumeric characters, "
                "underscores, and hyphens are allowed"
            )
        )
    return name


MIN_PASSWORD_LENGTH = 8


def validate_password_strength(password: str) -> str:
    """Validate password strength and return it if valid."""
    errors = []
    if len(password) < MIN_PASSWORD_LENGTH:
        errors.append(f"at least {MIN_PASSWORD_LENGTH} characters")
    if not re.search(r"[A-Z]", password):
        errors.append("an uppercase letter")
    if not re.search(r"[a-z]", password):
        errors.append("a lowercase letter")
    if not re.search(r"\d", password):
        errors.append("a digit")
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>\-_=+\[\]\\;'/`~]", password):
        errors.append("a special character")
    if errors:
        raise ValidationError(detail=f"Password must contain {', '.join(errors)}")
    return password


def contains_special_characters(
    s, allow_spaces=True, allow_underscores=True, allow_special_chars=True
):
    """Check if a string contains special characters based on given rules."""
    if s.strip() == "":
        return True

    if "$" in s:
        return True

    if allow_special_chars:
        return False

    pattern = (
        (r"[^a-zA-Z0-9_ ]" if allow_spaces else r"[^a-zA-Z0-9_]")
        if allow_underscores
        else r"[^a-zA-Z0-9]"
    )

    return bool(re.search(pattern, s))

def include_object(object, name, type_, reflected, compare_to):
    """
    An util for Alembic to determine whether an object should be included based on its type and schema.

    Args:
        object: The object to be evaluated.
        name: The name of the object (unused in the decision logic).
        type_: The type of the object (e.g., "table").
        reflected: A flag indicating if the object is reflected (unused in the decision logic).
        compare_to: A comparison target (unused in the decision logic).

    Returns:
        bool: True if the object should be included, False otherwise.
              Specifically, returns False for tables not in the "listeners" schema,
              and True for all other cases.
    """
    if type_ == "table" and object.schema != "listeners":
        return False

    return True

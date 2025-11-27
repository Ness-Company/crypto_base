from alembic import context


def include_object(object, name, type_, reflected, compare_to):
    """
    Utility for Alembic to decide whether to include an object according to its type and schema.

    Args:
        object: The object to be evaluated.
        name: The name of the object (unused in the decision logic).
        type_: The type of the object (e.g., "table").
        reflected: A flag indicating if the object is reflected (unused in the decision logic).
        compare_to: A comparison target (unused in the decision logic).

    Returns:
        bool: True if the object should be included, False otherwise.
              Specifically, returns False for tables whose schema does not match
              the schema specified via Alembic's x-argument, and True for all other cases.
    """
    return not (type_ == "table" and object.schema != context.get_x_argument(as_dictionary=True).get("schema", None))

"""Synchronous source streams using the libraries' existing execution pipelines.

The source-field lookup follows graphql-core 3.2's execute_subscription:
https://github.com/graphql-python/graphql-core/blob/v3.2.6/src/graphql/execution/subscribe.py
Only its calling convention and iterator requirement change.
The upstream MIT notice is retained in GRAPHQL_CORE_LICENSE.txt.
"""

from collections.abc import Mapping
from inspect import isawaitable

from graphql import GraphQLError, located_error
from graphql.execution.collect_fields import collect_fields
from graphql.execution.execute import get_field_def
from graphql.execution.values import get_argument_values
from graphql.language import OperationType
from graphql.pyutils import Path


def source_context(base, holder):
    """Keep parsing, validation, variable coercion and extension hooks upstream."""

    class SourceContext(base):
        def execute_operation(self, operation, root_value):
            if operation.operation != OperationType.SUBSCRIPTION:
                return super().execute_operation(operation, root_value)
            holder.append(None)
            root_type = self.schema.subscription_type
            if root_type is None:
                raise GraphQLError("Schema has no subscription root.")
            fields = collect_fields(
                self.schema, self.fragments, self.variable_values, root_type,
                operation.selection_set,
            )
            if len(fields) != 1:
                raise GraphQLError("A subscription must select one root field.")
            name, nodes = next(iter(fields.items()))
            field = get_field_def(self.schema, root_type, nodes[0])
            info = self.build_resolve_info(field, nodes, root_type, Path(None, name, root_type.name))
            args = get_argument_values(field, nodes[0], self.variable_values)
            resolver = field.subscribe or self.subscribe_field_resolver
            try:
                stream = resolver(root_value, info, **args)
                if isinstance(stream, Exception):
                    raise stream
                if isawaitable(stream):
                    if hasattr(stream, "close"):
                        stream.close()
                    raise TypeError("function-profile subscriptions must return an ordinary iterator, not an awaitable")
                if hasattr(stream, "__aiter__") or isinstance(stream, (str, bytes, Mapping)):
                    raise TypeError("function-profile subscriptions must return an ordinary iterator")
                holder[0] = iter(stream)
            except Exception as error:
                raise located_error(error, nodes, info.path.as_list()) from error
            return {}

    return SourceContext

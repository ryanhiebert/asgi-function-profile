"""Optional Strawberry Django function-profile WebSocket integration."""

from copy import copy
from strawberry.django.views import GraphQLView
from strawberry.django.context import StrawberryDjangoContext
from django.http import HttpResponse

from .graphql_execution import source_context
from .graphql_websocket import DjangoGraphQLWebSocket


class StrawberryDjangoWebSocket(DjangoGraphQLWebSocket):
    """Use ordinary Strawberry subscription functions returning iterators.

    Specify the event type with @strawberry.subscription(graphql_type=...).
    Existing async generators are deliberately rejected.
    """

    def default_context(self, request):
        return StrawberryDjangoContext(request=request, response=HttpResponse())

    def execute(self, query, *, source=None, **kwargs):
        schema = self.schema
        if source is not None:
            # A per-operation shallow copy avoids changing the shared schema.
            schema = copy(schema)
            schema.execution_context_class = source_context(schema.execution_context_class, source)
        result = schema.execute_sync(query, **kwargs)
        if result.errors:
            # The SDK patches the view's error hook, rather than execute_sync.
            # Preserve that existing hook instead of duplicating SDK capture.
            GraphQLView(schema=schema)._handle_errors(result.errors, self.format_result(result))
        return result

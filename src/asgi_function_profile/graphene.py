"""Optional Graphene Django function-profile WebSocket integration."""

from graphql.execution import ExecutionContext
from graphene_django.settings import graphene_settings
from graphene_django.views import instantiate_middleware

from .graphql_execution import source_context
from .graphql_websocket import DjangoGraphQLWebSocket


class GrapheneDjangoWebSocket(DjangoGraphQLWebSocket):
    """Use a Graphene schema with ordinary subscribe_<field> iterators."""

    def __init__(self, schema, *, middleware=None, **kwargs):
        super().__init__(schema, **kwargs)
        if middleware is None:
            middleware = graphene_settings.MIDDLEWARE
        self.middleware = tuple(middleware or [])

    def execute(self, query, *, source=None, **kwargs):
        kwargs["middleware"] = list(instantiate_middleware(self.middleware))
        if source is not None:
            kwargs["execution_context_class"] = source_context(ExecutionContext, source)
        return self.schema.execute(query, **kwargs)

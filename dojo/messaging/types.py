from starlette.routing import Mount, Route, WebSocketRoute

RouteType = Route | WebSocketRoute | Mount

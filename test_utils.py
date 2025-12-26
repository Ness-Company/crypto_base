import grpc


class GrpcFakeContext:
    def __init__(self):
        self.code = grpc.StatusCode.OK
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details

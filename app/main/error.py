from zope.interface import implementer

class AuthorizedError(Exception):
    pass


class AuthenticatedError(Exception):
    pass


class PaymentError(Exception):
    pass

#
# @implementer(IResource)
# class UnAthenticatedResourceNoWwwAuthenticate(object):
#     isLeaf = True
#
#     def render(self, request):
#         request.setResponseCode(401)
#         if request.method == b'HEAD':
#             return b''
#         return b'Unauthorized'
#
#     def getChildWithDefault(self, path, request):
#         """
#         Disable resource dispatch
#         """
#         return self

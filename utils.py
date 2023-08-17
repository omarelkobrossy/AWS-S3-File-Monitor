import validators
import argparse
import os

def detect_resource_type(url):
    if "s3" in url and "amazonaws" in url:
        return "S3 Bucket"
    return "Unknown Resource Type"

#Custom validation function for paths
def validate_local_path(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"'{path}' does not exist")
    return path

# Custom validation function for URLs
def validate_url(url):
    # Add a default scheme if none is provided
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    if not validators.url(url):
        raise argparse.ArgumentTypeError(f"'{url}' is not a valid URL")
    return url


def get_status_text(status_code):
    status_codes = {
        100: "Continue",
        101: "Switching Protocols",
        200: "OK",
        201: "Created",
        202: "Accepted",
        203: "Non-Authoritative Information",
        204: "No Content",
        205: "Reset Content",
        206: "Partial Content",
        300: "Multiple Choices",
        301: "Moved Permanently",
        302: "Found",
        303: "See Other",
        304: "Not Modified",
        305: "Use Proxy",
        307: "Temporary Redirect",
        308: "Permanent Redirect",
        400: "Bad Request",
        401: "Unauthorized",
        402: "Payment Required",
        403: "Forbidden",
        404: "Not Found",
        405: "Method Not Allowed",
        406: "Not Acceptable",
        407: "Proxy Authentication Required",
        408: "Request Timeout",
        409: "Conflict",
        410: "Gone",
        411: "Length Required",
        412: "Precondition Failed",
        413: "Payload Too Large",
        414: "URI Too Long",
        415: "Unsupported Media Type",
        416: "Range Not Satisfiable",
        417: "Expectation Failed",
        418: "I'm a teapot",
        421: "Misdirected Request",
        422: "Unprocessable Entity",
        423: "Locked",
        424: "Failed Dependency",
        426: "Upgrade Required",
        428: "Precondition Required",
        429: "Too Many Requests",
        431: "Request Header Fields Too Large",
        451: "Unavailable For Legal Reasons",
        500: "Internal Server Error",
        501: "Not Implemented",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
        505: "HTTP Version Not Supported",
        506: "Variant Also Negotiates",
        507: "Insufficient Storage",
        508: "Loop Detected",
        510: "Not Extended",
        511: "Network Authentication Required"
    }
    return status_codes.get(status_code, "Unknown Status Code")

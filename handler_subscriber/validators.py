from urllib.parse import urlparse




def extract_dataset_from_url(url):

    parsed = urlparse(url)
    path = parsed.path
    prefix = "/dataset/"
    if path.startswith(prefix):
        return path[len(prefix):]
    return None

def valid_theme(input,superthemes):
        if input.lower() in superthemes:
            return True
        else:
            return False

def valid_dataset(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if parsed.netloc != "datos.gob.ar":
            return False
        if not parsed.path.startswith("/dataset/"):
            return False
        return True
    except:
        return False


def valid_nodo(input, nodos):
    if input.lower() in nodos:
        return True
    else:
        return False


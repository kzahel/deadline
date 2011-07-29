def encode_multipart_formdata(fields):
    """
    fields is a sequence of (name, value) elements for regular form fields.
    Return (content_type, body) ready for httplib.HTTP instance
    """
    if len(fields) == 0:
        return None, ''
    BOUNDARY = '--tiEEnybdry_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + BOUNDARY)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)
    L.append('--' + BOUNDARY + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % BOUNDARY
    return content_type, body

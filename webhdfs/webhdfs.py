import re
import json
import logging
import zlib

from httplib import HTTPConnection
from urlparse import urlparse

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='webhdfs')

WEBHDFS_CONTEXT_ROOT = "/webhdfs/v1"
ZLIB_WBITS = 16


class _NameNodeHTTPClient():
    def __init__(self, http_method, url_path, namenode_host, namenode_port, hdfs_username, headers={}):
        url_path += '&user.name=' + hdfs_username
        self.httpclient = HTTPConnection(namenode_host, namenode_port, timeout=600)
        self.httpclient.request(http_method, url_path, headers=headers)

    def __enter__(self):
        response = self.httpclient.getresponse()
        logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
        return response

    def __exit__(self, type, value, traceback):
        self.httpclient.close()


class WebHDFS(object):
    """ Class for accessing HDFS via WebHDFS

        To enable WebHDFS in your Hadoop Installation add the following configuration
        to your hdfs_site.xml (requires Hadoop >0.20.205.0):

        <property>
             <name>dfs.webhdfs.enabled</name>
             <value>true</value>
        </property>

        see: https://issues.apache.org/jira/secure/attachment/12500090/WebHdfsAPI20111020.pdf
    """


    def __init__(self, namenode_host, namenode_port, hdfs_username):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.username = hdfs_username

    def parse_url(self, url):
        (scheme, netloc, path, params, query, frag) = urlparse(url)

        # Verify hostnames are valid and parse a port spec (if any)
        match = re.match('([a-zA-Z0-9\-\.]+):?([0-9]{2,5})?', netloc)

        if match:
            (host, port) = match.groups()
        else:
            raise Exception('Invalid host and/or port: %s' % netloc)

        return host, int(port), path.strip('/'), query

    def mkdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=MKDIRS'
        logger.debug("Create directory: " + url_path)
        with _NameNodeHTTPClient('PUT', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            return json.loads(response.read())

    def rmdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=DELETE&recursive=true'
        logger.debug("Delete directory: " + url_path)
        with _NameNodeHTTPClient('DELETE', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            return json.loads(response.read())

    def copyfromlocal(self, source_path, target_path, replication=1, overwrite=True):
        url_path = WEBHDFS_CONTEXT_ROOT + target_path + '?op=CREATE&overwrite=' + 'true' if overwrite else 'false'

        with _NameNodeHTTPClient('PUT', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            redirect_location = response.msg["location"]
            logger.debug("HTTP Location: %s" % redirect_location)
            (redirect_host, redirect_port, redirect_path, query) = self.parse_url(redirect_location)

            # Bug in WebHDFS 0.20.205 => requires param otherwise a NullPointerException is thrown
            redirect_path = redirect_path + "?" + query + "&replication=" + str(replication)

            logger.debug("Redirect: host: %s, port: %s, path: %s " % (redirect_host, redirect_port, redirect_path))
            fileUploadClient = HTTPConnection(redirect_host, redirect_port, timeout=600)

            # This requires currently Python 2.6 or higher
            fileUploadClient.request('PUT', redirect_path, open(source_path, "r").read(), headers={})
            response = fileUploadClient.getresponse()
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            fileUploadClient.close()

            return json.loads(response.read())

    def copytolocal(self, source_path, target_path):
        url_path = WEBHDFS_CONTEXT_ROOT + source_path + '?op=OPEN&overwrite=true'
        logger.debug("GET URL: %s" % url_path)

        with _NameNodeHTTPClient('GET', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            # if file is empty GET returns a response with length == NONE and no msg["location"]
            if response.length is not None and response.msg:
                redirect_location = response.msg["location"]
                logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
                logger.debug("HTTP Location: %s" % redirect_location)

                (redirect_host, redirect_port, redirect_path, query) = self.parse_url(redirect_location)

                redirect_path = redirect_path + "?" + query

                logger.debug("Redirect: host: %s, port: %s, path: %s " % (redirect_host, redirect_port, redirect_path))
                fileDownloadClient = HTTPConnection(redirect_host, redirect_port, timeout=600)

                fileDownloadClient.request('GET', redirect_path, headers={})
                response = fileDownloadClient.getresponse()
                logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))

                with open(target_path, "w") as f:
                    f.write(response.read())

                fileDownloadClient.close()
            else:
                with open(target_path, "w") as f:
                    f.write("")

            return response.status

    def readfile(self, source_path, offset=0, length=10000, buffersize=10000):
        url_path = WEBHDFS_CONTEXT_ROOT + source_path + '?op=OPEN&offset=%s&length=%s&buffersize=%s' % (
            offset, length, buffersize)

        logger.debug("GET URL: %s" % url_path)

        with _NameNodeHTTPClient('GET', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            # if file is empty GET returns a response with length == NONE and no msg["location"]
            if response.length is not None and response.msg:
                redirect_location = response.msg["location"]
                logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
                logger.debug("HTTP Location: %s" % redirect_location)
                (redirect_host, redirect_port, redirect_path, query) = self.parse_url(redirect_location)

                redirect_path = redirect_path + "?" + query

                logger.debug("Redirect: host: %s, port: %s, path: %s " % (redirect_host, redirect_port, redirect_path))

                with _NameNodeHTTPClient('GET', url_path, redirect_host, redirect_port, self.username) \
                        as redirect_response:
                    logger.debug("HTTP Response: %d, %s" % (redirect_response.status, redirect_response.reason))
                    if source_path.endswith('.gz'):  #TODO: Fixme
                        data = redirect_response.read()  # redirect_response.read().decode("zlib") <-- breaks
                        return zlib.decompressobj(ZLIB_WBITS).decompress(data, length)  # decompresing lenght only
                    else:
                        return redirect_response.read()

    def listdir(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=LISTSTATUS'
        logger.debug("List directory: " + url_path)

        with _NameNodeHTTPClient('GET', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            data_dict = json.loads(response.read())
            logger.debug("Data: " + str(data_dict))

            files = []
            for i in data_dict["FileStatuses"]["FileStatus"]:
                logger.debug(i["type"] + ": " + i["pathSuffix"])
                files.append((i["pathSuffix"], i["length"], i["type"]))

        return files

    def getfilestatus(self, path):
        url_path = WEBHDFS_CONTEXT_ROOT + path + '?op=GETFILESTATUS'
        logger.debug("List directory: " + url_path)

        with _NameNodeHTTPClient('GET', url_path, self.namenode_host, self.namenode_port, self.username) as response:
            logger.debug("HTTP Response: %d, %s" % (response.status, response.reason))
            data_dict = json.loads(response.read())
            logger.debug("Data: " + str(data_dict))
        return data_dict.get('FileStatus')


if __name__ == "__main__":
    webhdfs = WebHDFS("namenode.hadoop.staging.corp", 50070, "hadoop_user")
    print webhdfs.listdir("/data/product")
    print webhdfs.readfile("/data/product/file.log-2013-05-31.gz")

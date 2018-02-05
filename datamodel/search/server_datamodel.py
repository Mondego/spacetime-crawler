from __future__ import absolute_import
import logging
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.parameter import parameter, ParameterMode
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.projection import projection
from rtypes.pcc.attributes import dimension, primarykey, count
from rtypes.pcc.types.impure import impure
import socket, base64, requests, uuid

from urllib2 import Request, urlopen, HTTPError, URLError
from urlparse import urlparse, parse_qs
import httplib

from datamodel.search.Robot import Robot

robot_manager = Robot()


class UrlResponse(object):
    def __init__(
            self, url, content, error_message,
            http_code, headers, is_redirected, final_url=None):
        self.url = url
        self.content = content
        self.error_message = error_message
        self.headers = headers
        self.http_code = http_code
        self.is_redirected = is_redirected
        self.final_url = final_url


class Link(object):
    @primarykey(str)
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @dimension(str)
    def raw_content(self): 
        try:
            return self._rc
        except AttributeError:
            return None

    @raw_content.setter
    def raw_content(self, value):
        self._rc = value

    @dimension(str)
    def scheme(self):
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        self._scheme = value

    @dimension(str)
    def domain(self):
        return self._domain

    @domain.setter
    def domain(self, value):
        self._domain = value

    @dimension(str)
    def http_code(self):
        try:
            return self._http_code
        except AttributeError:
            return 400 # Error and unknown.


    @http_code.setter
    def http_code(self, value):
        self._http_code = value

    @dimension(str)
    def error_reason(self):
        try:
            return self._error_reason
        except AttributeError:
            return None

    @error_reason.setter
    def error_reason(self, value):
        self._error_reason = str(value) if value is not None else None

    @dimension(dict)
    def http_headers(self):
        try:
            return self._http_headers
        except AttributeError:
            return dict()

    @http_headers.setter
    def http_headers(self, v):
        self._http_headers = dict(v)

    @dimension(bool)
    def is_redirected(self):
        try:
            return self._is_redirect
        except AttributeError:
            return False

    @is_redirected.setter
    def is_redirected(self, v):
        self._is_redirect = v

    @dimension(str)
    def final_url(self):
        try:
            return self._final_url
        except AttributeError:
            return ""

    @final_url.setter
    def final_url(self, v):
        self._final_url = v

    @property
    def full_url(self): return self.scheme + "://" + self.url

    @dimension(bool)
    def download_complete(self):
        return self._dc

    @download_complete.setter
    def download_complete(self, v):
        self._dc = v

    def __ProcessUrlData(self, raw_content, useragentstr):
        self.raw_content = raw_content
        self.download_complete = True
        return UrlResponse(
            self.full_url, self.raw_content, self.error_reason,
            self.http_code, self.http_headers, self.is_redirected,
            self.final_url)

    def __init__(self, produced_link):
        pd = urlparse(produced_link)
        if pd.path:
            path = pd.path[:-1] if pd.path[-1] == "/" else pd.path
        else:
            path = ""
        self.url = pd.netloc + path + (("?" + pd.query) if pd.query else "")
        self.scheme = pd.scheme
        self.domain = pd.hostname
        self.download_complete = False

    def download(self, timeout=2,
                 MaxPageSize=1048576, MaxRetryDownloadOnFail=5, retry_count=0):
        url = self.full_url
        self.download_complete = True
        if self.raw_content != None:
            print ("Downloading " + url + " from cache.")
            return UrlResponse(
                url, self.raw_content, self.error_reason, self.http_code,
                self.http_headers, self.is_redirected, self.final_url)
        else:
            try:
                print ("Downloading " + url + " from source.")
            except Exception:
                pass
            try:
                urlresp = requests.get(
                    url,
                    timeout=timeout, 
                    headers={"user-agent" : self.user_agent_string})

                self.http_code = urlresp.status_code
                self.is_redirected = len(urlresp.history) > 0
                self.final_url = urlresp.url if self.is_redirected else None
                urlresp.raise_for_status()
                self.http_headers = dict(urlresp.headers)
                try:
                    size = int(urlresp.headers.get("Content-Length"))
                except TypeError:
                    size = -1
                except AttributeError:
                    size = -1
                except IndexError:
                    size = -1
                try:
                    content_type = urlresp.headers.get("Content-Type")
                    mime = content_type.strip().split(";")[0].strip().lower()
                    if mime not in ["text/plain", "text/html",
                                    "application/xml"]:
                        self.error_reason = "Mime does not match"
                        return UrlResponse(
                            url, "", self.error_reason, self.http_code,
                            self.http_headers, self.is_redirected,
                            self.final_url)
                except Exception:
                    pass
                if (size < MaxPageSize
                        and urlresp.status_code > 199
                        and urlresp.status_code < 300):
                    return self.__ProcessUrlData(
                        urlresp.text.encode("utf-8"), self.user_agent_string)
                elif size >= MaxPageSize:
                    self.error_reason = "Size too large."
                    return UrlResponse(
                        url, "", self.error_reason, self.http_code,
                        self.http_headers, self.is_redirected, self.final_url)

            except requests.HTTPError, e:
                self.http_code = 400
                self.error_reason = str(urlresp.reason)
                return UrlResponse(
                    url, "", self.error_reason, self.http_code,
                    self.http_headers, self.is_redirected, self.final_url)
            except socket.error:
                if (retry_count == MaxRetryDownloadOnFail):
                    self.http_code = 400
                    self.error_reason = "Socket error. Retries failed."
                    return UrlResponse(
                        url, "", self.error_reason, self.http_code,
                        self.http_headers, self.is_redirected, self.final_url)
                try:
                    print (
                        "Retrying " + url + " "
                        + str(retry_count + 1) + " time")
                except Exception:
                    pass
                return self.download(
                    timeout, MaxPageSize,
                    MaxRetryDownloadOnFail, retry_count + 1)
            except requests.ConnectionError, e:
                self.http_code = 499
                self.error_reason = str(e.message)
            except requests.RequestException, e:
                self.http_code = 499
                self.error_reason = str(e.message)
            #except Exception, e:
            #    # Can throw unicode errors and others... don't halt the thread
            #    self.error_reason = "Unknown error: " + str(e.message)
            #    self.http_code = 499
            #    print(type(e).__name__ + " occurred during URL Fetching.")
        return UrlResponse(
            url, "", self.error_reason, self.http_code,
            self.http_headers, self.is_redirected, self.final_url)

    def copy_from(self, link_obj):
        self.url = link_obj.url
        self.domain = link_obj.domain
        self.http_code = link_obj.http_code
        self.http_headers = link_obj.http_headers
        self.is_redirected = link_obj.is_redirected
        self.raw_content = link_obj.raw_content
        self.final_url = link_obj.final_url
        self.scheme = link_obj.scheme


@pcc_set
class ServerCopy(Link):
    def __init__(self, link_obj):
        self.copy_from(link_obj)

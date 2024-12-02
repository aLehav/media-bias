"""Advertools page with timeout arg and error catching added. Filtering added"""

import logging
from concurrent import futures
from gzip import GzipFile
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from urllib.error import URLError
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import pandas as pd

from advertools import __version__ as version

if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO)


headers = {"User-Agent": "advertools-" + version}
MAX_TIME = 60 # Used in timeout param

def _sitemaps_from_robotstxt(robots_url, request_headers):
    sitemaps = []
    robots_page = urlopen(Request(robots_url, headers=request_headers), timeout=MAX_TIME)
    for line in robots_page.readlines():
        line_split = [s.strip() for s in line.decode().split(":", maxsplit=1)]
        if line_split[0].lower() == "sitemap":
            sitemaps.append(line_split[1])
    return sitemaps


def _parse_sitemap(root):
    d = dict()
    for node in root:
        for n in node:
            if "loc" in n.tag:
                d[n.text] = {}

    def parse_xml_node(node, node_url, prefix=""):
        nonlocal d
        keys = []
        for element in node:
            if element.text:
                tag = element.tag.split("}")[-1]
                d[node_url][prefix + tag] = element.text
                keys.append(tag)
                prefix = prefix if tag in keys else ""
            if list(element):
                parse_xml_node(
                    element, node_url, prefix=element.tag.split("}")[-1] + "_"
                )

    for node in root:
        node_urls = [n.text for n in node if "loc" in n.tag]
        if not node_urls:
            logging.warning(f"No <loc> tag found in a sitemap node: {node}")
            continue  # Skip nodes without a <loc> tag
        node_url = node_urls[0]
        parse_xml_node(node, node_url=node_url)
    return pd.DataFrame(d.values())


def _build_request_headers(user_headers=None):
    # Must ensure lowercase to avoid introducing duplicate keys
    final_headers = {key.lower(): val for key, val in headers.items()}
    if user_headers:
        user_headers = {key.lower(): val for key, val in user_headers.items()}
        final_headers.update(user_headers)
    return final_headers

def _filter_sitemap(sitemap_url, last_scraped):
    if last_scraped:
        parsed_url = urlparse(sitemap_url)
        query_params = parse_qs(parsed_url.query)

        if "date" in query_params:
            try:
                date_param = datetime.strptime(query_params["date"][0], "%Y-%m-%d")
                if date_param < last_scraped:
                    logging.info(f"Ignoring sitemap {sitemap_url} due to date filter.")
                    return False
            except ValueError:
                pass  # Ignore parsing errors and proceed

    return True

def sitemap_to_df(sitemap_url, max_workers=8, recursive=True, request_headers=None, last_scraped=None):
    """
    Retrieve all URLs and other available tags of a sitemap(s) and put them in
    a DataFrame.

    You can also pass the URL of a sitemap index, or a link to a robots.txt
    file.

    :param url sitemap_url: The URL of a sitemap, either a regular sitemap, a
                            sitemap index, or a link to a robots.txt file.
                            In the case of a sitemap index or robots.txt, the
                            function will go through all the sub sitemaps and
                            retrieve all the included URLs in one DataFrame.
    :param int max_workers: The maximum number of workers to use for threading.
                            The higher the faster, but with high numbers you
                            risk being blocked and/or missing some data as you
                            might appear like an attacker.
    :param bool recursive: Whether or not to follow and import all sub-sitemaps
                           (in case you have a sitemap index), or to only
                           import the given sitemap. This might be useful in
                           case you want to explore what sitemaps are available
                           after which you can decide which ones you are
                           interested in.
    :param dict request_headers: One or more request headers to use while
                                 fetching the sitemap.
    :return sitemap_df: A pandas DataFrame containing all URLs, as well as
                        other tags if available (``lastmod``, ``changefreq``,
                        ``priority``, or others found in news, video, or image
                        sitemaps).
    """
    final_headers = _build_request_headers(request_headers)

    try:
        if sitemap_url.endswith("robots.txt"):
            return pd.concat(
                [
                    sitemap_to_df(sitemap, recursive=recursive, last_scraped=last_scraped)
                    for sitemap in _sitemaps_from_robotstxt(sitemap_url, final_headers)
                ],
                ignore_index=True,
            )
        
        if not _filter_sitemap(sitemap_url, last_scraped):
            return pd.DataFrame()

        if sitemap_url.endswith("xml.gz"):
            final_headers["accept-encoding"] = "gzip"
            xml_text = urlopen(Request(sitemap_url, headers=final_headers), timeout=MAX_TIME)
            try:
                resp_headers = xml_text.getheaders()
            except AttributeError:
                resp_headers = ""
                pass
            xml_text = GzipFile(fileobj=xml_text)
        else:
            xml_text = urlopen(Request(sitemap_url, headers=final_headers), timeout=MAX_TIME)
            try:
                resp_headers = xml_text.getheaders()
            except AttributeError:
                resp_headers = ""
                pass
        xml_string = xml_text.read()
        root = ElementTree.fromstring(xml_string)
    except URLError as e:
        if hasattr(e, "reason") and isinstance(e.reason, TimeoutError):
            logging.warning(f"Timeout error while accessing {sitemap_url}: {e}")
        else:
            logging.warning(f"Error while accessing {sitemap_url}: {e}")
        return pd.DataFrame({"sitemap": [sitemap_url], "errors": [str(e)]})

    sitemap_df = pd.DataFrame()

    if (root.tag.split("}")[-1] == "sitemapindex") and recursive:
        multi_sitemap_df = pd.DataFrame()
        sitemap_url_list = []
        for elem in root:
            for el in elem:
                if "loc" in el.tag:
                    if el.text == sitemap_url:
                        error_df = pd.DataFrame(
                            {
                                "sitemap": [sitemap_url],
                                "errors": [
                                    "WARNING: Sitemap contains a link to itself"
                                ],
                            }
                        )
                        multi_sitemap_df = pd.concat(
                            [multi_sitemap_df, error_df], ignore_index=True
                        )
                    else:
                        sitemap_url_list.append(el.text)
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            to_do = []
            for sitemap in sitemap_url_list:
                future = executor.submit(sitemap_to_df, sitemap, last_scraped=last_scraped)
                to_do.append(future)
            done_iter = futures.as_completed(to_do)
            for future in done_iter:
                try:
                    multi_sitemap_df = pd.concat(
                        [multi_sitemap_df, future.result()], ignore_index=True
                    )
                except Exception as e:
                    error_df = pd.DataFrame(dict(errors=str(e)), index=range(1))
                    future_str = hex(id(future))
                    hexes = [hex(id(f)) for f in to_do]
                    index = hexes.index(future_str)
                    error_df["sitemap"] = sitemap_url_list[index]
                    logging.warning(msg=str(e) + " " + sitemap_url_list[index])
                    multi_sitemap_df = pd.concat(
                        [multi_sitemap_df, error_df], ignore_index=True
                    )
        return multi_sitemap_df

    else:
        logging.info(msg="Getting " + sitemap_url)
        elem_df = _parse_sitemap(root)
        sitemap_df = pd.concat([sitemap_df, elem_df], ignore_index=True)
        sitemap_df["sitemap"] = [sitemap_url] if sitemap_df.empty else sitemap_url
    if "lastmod" in sitemap_df:
        try:
            sitemap_df["lastmod"] = pd.to_datetime(sitemap_df["lastmod"], utc=True)
            if last_scraped:
                sitemap_df = sitemap_df[sitemap_df["lastmod"] >= last_scraped]
        except Exception:
            pass
    if "priority" in sitemap_df:
        try:
            sitemap_df["priority"] = sitemap_df["priority"].astype(float)
        except Exception:
            pass
    if resp_headers:
        etag_lastmod = {
            header.lower().replace("-", "_"): val
            for header, val in resp_headers
            if header.lower() in ["etag", "last-modified"]
        }
        sitemap_df = sitemap_df.assign(**etag_lastmod)
    if "last_modified" in sitemap_df:
        sitemap_df["sitemap_last_modified"] = pd.to_datetime(
            sitemap_df["last_modified"]
        )
        del sitemap_df["last_modified"]
    sitemap_df["sitemap_size_mb"] = len(xml_string) / 1024 / 1024
    sitemap_df["download_date"] = pd.Timestamp.now(tz="UTC")
    return sitemap_df
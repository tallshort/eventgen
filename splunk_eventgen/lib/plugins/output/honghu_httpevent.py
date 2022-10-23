import random
from splunk_eventgen.lib.logging_config import logger
from splunk_eventgen.lib.plugins.output.httpevent_core import HTTPCoreOutputPlugin

try:
    import ujson as json
except ImportError:
    import json


class NoServers(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class BadConnection(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class HonghuHTTPEventOutputPlugin(HTTPCoreOutputPlugin):
    """
    HTTPEvent output will enable events that are generated to be sent directly
    to Honghu through the HTTP event input.  In order to use this output plugin,
    you will need to supply the following attributes in the eventgen.conf:
    httpeventServers = { "servers": [ { "url": "xxx", "token": "yyy" } ] }
    httpeventUrlParams = {"endpoint": "test_hei_input", "event_set": "test_eventset", "_datatype": "json", "_source": "eventgen", "_host": "test_host"}
    """

    name = "honghu_httpevent"

    def __init__(self, sample, output_counter=None):
        super(HonghuHTTPEventOutputPlugin, self).__init__(sample, output_counter)

    def updateConfig(self, config):
        HTTPCoreOutputPlugin.updateConfig(self, config)
        if not hasattr(self._sample, "httpeventUrlParams"):
            raise AttributeError(
                "outputMode %s but httpeventUrlParams not specified for sample %s"
                % (self.name, self._sample.name)
            )

    def createConnections(self):
        self.serverPool = []
        if self.httpeventServers:
            for server in self.httpeventServers.get("servers"):
                if not server.get("url"):
                    logger.error(
                        "requested a connection to a httpevent server, but no url specified for sample %s"
                        % self._sample.name
                    )
                    raise ValueError(
                        "requested a connection to a httpevent server, but no url specified for sample %s"
                        % self._sample.name
                    )
                if not server.get("token"):
                    logger.error(
                        "requested a connection to a httpevent server, but no token specified for server %s"
                        % server
                    )
                    raise ValueError(
                        "requested a connection to a httpevent server, but no token specified for server %s"
                        % server
                    )
                logger.debug(
                    "Validation Passed, Creating a requests object for server url: %s"
                    % server.get("url")
                )

                setserver = {}
                setserver["url"] = server.get("url")
                setserver["header"] = "Bearer %s" % server.get("token")
                logger.debug("Adding server set to pool, server: %s" % setserver)
                self.serverPool.append(setserver)
        else:
            raise NoServers(
                "outputMode %s but httpeventServers not specified for sample %s"
                % (self.name, self._sample.name)
            )

    def flush(self, q):
        logger.error("Flush called on honghu_httpevent plugin")
        self._setup_REST_workers()

        if len(q) > 0:
            try:
                payload = []
                logger.debug("Currently being called with %d events" % len(q))
                for event in q:
                    logger.debug("HTTPEvent proccessing event: %s" % event)
                    if event.get("_raw") is None or event["_raw"] == "\n":
                        logger.error("failure outputting event, does not contain _raw")
                    else:
                        logger.debug("Event contains _raw, attempting to process...")
                        payload.append(event["_raw"])

                logger.debug("Finished processing events, sending all to Honghu")
                self._sendHTTPEvents(payload)
                payload = []

                if self.config.httpeventWaitResponse:
                    for session in self.active_sessions:
                        response = session.result()
                        if not response.raise_for_status():
                            logger.debug(
                                "Payload successfully sent to httpevent server."
                            )
                        else:
                            logger.error(
                                "Server returned an error while trying to send, response code: %s"
                                % response.status_code
                            )
                            raise BadConnection(
                                "Server returned an error while sending, response code: %s"
                                % response.status_code
                            )
                else:
                    logger.debug(
                        "Ignoring response from HTTP server, leaving httpevent outputter"
                    )
            except Exception as e:
                logger.error("failed indexing events, reason: %s " % e)

    def _sendHTTPEvents(self, payload):
        numberevents = len(payload)
        logger.debug("Sending %s events to Honghu" % numberevents)
        for rawevent in payload:
            logger.debug("stringpayload: %s " % rawevent)
            try:
                self._transmitEvent(rawevent)
            except Exception as e:
                logger.exception(str(e))
                raise e

    def _transmitEvent(self, payloadstring):
        targetServer = []
        logger.debug("Transmission called with payloadstring: %s " % payloadstring)

        if self.httpeventoutputmode == "mirror":
            targetServer = self.serverPool
        else:
            targetServer.append(random.choice(self.serverPool))

        for server in targetServer:
            logger.debug("Selected targetServer object: %s" % targetServer)
            url = server["url"]
            headers = {}
            headers["Authorization"] = server["header"]
            headers["content-type"] = "text/plain"
            try:
                params = json.loads(self._sample.httpeventUrlParams)
                logger.debug("Transmission called with url params: %s " % params)
                self.active_sessions.append(
                    self.session.post(
                        url=url, data=payloadstring, params=params, headers=headers, verify=False
                    )
                )
            except Exception as e:
                logger.error("Failed for exception: %s" % e)
                logger.debug(
                    "Failed sending events to url: %s  headers: %s payload: %s"
                    % (url, headers, payloadstring)
                )
                raise e

def load():
    """Returns an instance of the plugin"""
    return HonghuHTTPEventOutputPlugin


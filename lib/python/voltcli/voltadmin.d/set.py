# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import os
from voltcli import utility
from voltcli import properties
from voltcli import xmlproperties
from voltcli.hostinfo import Host
from voltcli.hostinfo import Hosts
from urllib2 import Request, urlopen, URLError
import base64

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Set a configuration property on a running database.',
    description2='Specify the property name and its value.',
    arguments=(
        VOLT.StringArgument(
            'property',
            'the property name',
            min_count=1, max_count=1),
        VOLT.StringArgument(
            'value',
            'the new value for the property',
            min_count=1, max_count=1),
    )
)
def set(runner):

    # Set up the properties
    properties.load_global_property_defs()

    # First parse the property and see if it makes sense.
    property_string = runner.opts.property + ": " + str(runner.opts.value)
    prop = properties.load_property_instance(property_string)
    
    if not prop.validate(4):    # condition 4= running database
        runner.abort("Cannot set the specified property.")
    
    print prop.definition
    print prop.value
    
    # Next, connect to the server and get the deployment file

    xmltext = get_deployment_file(runner)
    if not xmltext: runner.abort("Cannot retrieve current configuration.")
    
    xml = xmlproperties.loadxmlstring(xmltext)
    
    # Set the property
    xmlproperties.setxml(xml,"deployment." + prop.setdef, prop.value, prop.uniqueIDs)
    
    # Now reverse the process
    xmltext2 = xmlproperties.getxmldom(xml)
    print "DOM returned as:\n" + xmltext2

    #Apply the property



    # All the rest of this is what was left over from update...    
    columns = [VOLT.FastSerializer.VOLTTYPE_NULL, VOLT.FastSerializer.VOLTTYPE_STRING]
    catalog = None
    deployment = xmltext2
    #print "Property: "  + str(runner.opts.property)
    #print "Value: "  + str(runner.opts.value)
    
    params = [catalog, deployment]
    # call_proc() aborts with an error if the update failed.
    runner.call_proc('@UpdateApplicationCatalog', columns, params)
    runner.info('The catalog update succeeded.')

def get_deployment_file(runner):
    
    xmltext = get_http_url(runner)
    return xmltext
    
def get_http_url(runner):
    
    # Get some basic info
 
    return getCurrentDeploymentFile(runner)


# *************** THE FOLLOWING CODE WAS STOLEN AND HACKED FROM PLAN_UPGRADE.PY
#****************  AND SHOULD BE GENERALIZED IN RUNNER, PERHAPS?
# get deployment file through rest API
def getCurrentDeploymentFile(runner):
    sslContext = None
    if runner.opts.ssl_config is None:
        protocol = "http://"
    else:
        protocol = "https://"
        tlsv = None
        try:
            tlsv = ssl.PROTOCOL_TLSv1_2
        except AttributeError, e:
            print "WARNING: This version of python does not support TLSv1.2, upgrade to one that does"
            tlsv = ssl.PROTOCOL_TLSv1
        if ssl_available:
            sslContext = ssl.SSLContext(tlsv)
        else:
            print "ERROR: To use SSL functionality please Install the Python ssl module."
            raise ssl_exception
    url = protocol + runner.opts.host.host + ':' + "8080" + '/deployment/download/'
    request = Request(url)
    base64string = base64.b64encode('%s:%s' % (runner.opts.username, runner.opts.password))
    request.add_header("Authorization", "Basic %s" % base64string)
    try:
        if sslContext is None:
            response = urlopen(request)
        else:
            response = urlopen(request, context=sslContext)
    except URLError, e:
        runner.abort("Failed to get deployment file from %s " % (runner.opts.host.host))

    return response.read()
    

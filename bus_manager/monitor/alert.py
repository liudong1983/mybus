#!/usr/bin/env python26
# encoding: utf-8
import sys,getopt
import re
sys.path.append('/etc/dbCluster')
from  send_alert  import Client 

kid = "xxxxxxxxxx"
passwd = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
url_path = 'xxxxxxxxxxxx'

def usage(return_code=2):
    help = """
 Usage %s --service "service" --object "object" --subject "subject" --content "test" --gmsgto "litaotest" --gmailto "litaotest"  --auto_merge "0" [options]
 options:
  --service         service
  --object          object
  --subject         subject
  --content         content

  --gmailto         contact groups (split by comma)
  --gmsgto          contact groups (split by comma)
  --auto_merge      1/0
  --help            help usage

""" % sys.argv[0]
    print help
    sys.exit(return_code)


def send_alert(service,object,subject,content,gmsgto='',gmailto='',msgto='',auto_merge="1"):
    if gmsgto and msgto:
        print 'msgto and gsgto only one used'
        return 'msgto and gsgto only one used'
    if not service or not object or not subject or not content:
        print "4 para must given:service,object,subject,content"
        return '4 para must given:service,object,subject,content'
    elif not gmsgto and not gmailto and not msgto:
        print "3 prar must at least give 1:gmsgto and gmailto and msgto"
        return "3 prar must at least give 1:gmsgto and gmailto and msgto"
    else:
        data = dict(
            kid = kid,
            passwd = passwd,
            url = 'iconnect.monitor.sina.com.cn',
            sv = 'dsp',
            service = service,
            object = object,
            subject = subject,
            content = content,
            html = '1',
            mailto = '',
            msgto = msgto,
            ivrto = '',
            gmailto = gmailto,
            gmsgto = gmsgto,
            givrto = '',
            auto_merge = auto_merge,
        )
        client = Client(data['url'], 80)
        client.bind_auth(data.pop('kid'), data.pop('passwd'))
        response = client.post(url_path, data)
        #print response
        if response.split(',')[1].split(':')[1] == ' 0':
            return "1"
        else:
            return response

def test():
    service,object,subject,content,gmsgto,gmailto = ('auto_merge_test','object_test','suject_test','content_test','new_nosql','new_nosql')
    ret = send_alert(service,object,subject,content,gmsgto,gmailto)
    print ret
        
if __name__=="__main__":
    test()
    sys.exit()
    #print sys.argv 
    if len(sys.argv) < 2:
        usage()

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help","service=","object=","subject=","content=","gmailto=","gmsgto=","auto_merge="])
        print "opts:",opts
        #print "args:",args
        for o, a in opts: # [("service","sinadb3"),(xx)]
            if o in ("-h", "--help"):
                usage(0)
            elif o in ("--service"):
                service  = a
            elif o in ("--object"):
                object  = a
            elif o in ("--subject"):
                subject  = a
            elif o in ("--content"):
                content  = a
            elif o in ("--gmailto"):
                gmailto  = a
            elif o in ("--gmsgto"):
                gmsgto  = a
            elif o in ("--auto_merge"):
                auto_merge  = a
            else:
                print 'unknown %s!!!' % o
                usage()
        send_alert(service,object,subject,content,gmsgto,gmailto,auto_merge)
    except getopt.GetoptError, err:
        print str(err)
        usage()

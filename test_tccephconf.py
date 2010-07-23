from tccephconf import TCCephConf

def test_read(filename):
    f = open(filename, 'r')
    tmp = open('tmp', 'w')
    for line in f:
        tmp.write(line.lstrip())
    tmp.close()
    f.close()

def test_default():
    conf = TCCephConf()
    conf.create_default('hfaldjalfnvotij-fsd-dfvl')
    conf.add_mon('0', '10.201.193.140')
    print conf.get('mon.0', 'addr')
    print conf.get('tcloud', 'depot')
#    conf.read('tmp')
#    conf.add_section('tcloud')
#    conf.set('tcloud', 'depot id', 'hfaldjalfnvotij-fsd-dfvl')
#    for k, v in conf.defaults().iteritems():
#        print k, v
#        conf.set('global', k, v)
#    sections = conf.sections()
#    for s in sections:
#        print s
    with open('new.conf', 'wb') as outfile:
        conf.write(outfile)


if __name__ == '__main__':
    #test_read()
    test_default()
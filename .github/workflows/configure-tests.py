#!/usr/bin/python

import jinja2

JOBS = {
    'functional-tests': [
        {
            'name': 'debian-10-localhost',
            'baseimage': 'sf://label/system/sfci-debian-10',
            'baseuser': 'debian',
            'topology': 'localhost',
            'concurrency': 3
        },
        {
            'name': 'debian-11-slim-primary',
            'baseimage': 'sf://label/system/sfci-debian-11',
            'baseuser': 'debian',
            'topology': 'slim-primary',
            'concurrency': 5
        },
        {
            'name': 'ubuntu-2004-slim-primary',
            'baseimage': 'sf://label/system/sfci-ubuntu-2004',
            'baseuser': 'ubuntu',
            'topology': 'slim-primary',
            'concurrency': 5
        },
    ],
}


if __name__ == '__main__':
    for style in JOBS.keys():
        with open('%s.tmpl' % style) as f:
            t = jinja2.Template(f.read())

        for job in JOBS[style]:
            with open('%s-%s.yml' % (style, job['name']), 'w') as f:
                f.write(t.render(job))

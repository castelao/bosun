#!/usr/bin/env python

from __future__ import with_statement
import re
import os.path

from fabric.api import run, local, cd, settings, show, prefix, put
import fabric.colors as fc
from fabric.contrib.files import exists, comment
from fabric.decorators import task
import yaml


class NoEnvironmentSetException(Exception):
    pass


def env_options(f):
    def _wrapped_env(*args, **kw):
        exppath = kw.get('exppath', 'exp')
        filename = kw.get('filename', 'namelist.yaml')

        if args:
            environ = args[0]
        else:
            environ = _read_config(os.path.join(exppath, filename), updates=kw)

        if environ is None:
            raise NoEnvironmentSetException
        else:
            environ['exppath'] = exppath
            environ['filename'] = filename
            return f(environ, **kw)

    return _wrapped_env


def shell_env(args):
    env_vars = " ".join(["=".join((key, str(value))) for (key, value)
                                                     in args.items()])
    return prefix("export %s" % env_vars)


def _read_config(filename, updates=None):

    def rec_replace(env, key, value):
        finder = re.compile('\$\{(\w*)\}')
        ret_value = value

        try:
            keys = finder.findall(value)
        except TypeError:
            return value

        for k in keys:
            if k in env:
                ret_value = rec_replace(env, k,
                                        ret_value.replace('${%s}' % k, env[k]))

        return ret_value

    def env_replace(old_env):
        new_env = {}
        for k in old_env.keys():
            try:
                old_env[k].split(' ')
            except AttributeError:
                new_env[k] = old_env[k]
            else:
                new_env[k] = rec_replace(old_env, k, old_env[k])
        return new_env

    data = open(filename, 'r').read()
    TOKEN = re.compile(r'''\$\{(.*?)\}''')

    d = yaml.load(data)
    if updates:
        d.update(updates)
    #while set(TOKEN.findall(data)) & set(d.keys()):
    #    d = yaml.load(data)
    #    data = TOKEN.sub(lambda m: d.get(m.group(1),
    #                                     '${%s}' % m.group(1)), data)
    new_d = env_replace(d)

    # FIXME: sanitize input!
    return new_d


@env_options
@task
def instrument_code(environ, **kwargs):
    print(fc.yellow('Rebuilding executable with instrumentation'))
    with prefix('module load perftools'):
        clean_model_compilation(environ)
        compile_model(environ)
        run('pat_build -O {expdir}/instrument_coupler.apa '
            '-o {executable}+apa {executable}'.format(**environ))
        env['executable'] = ''.join((environ['executable'], '+apa'))


@env_options
@task
def run_model(environ, **kwargs):
    print(fc.yellow('Running model'))
    with shell_env(environ):
        with cd('{expdir}/runscripts'.format(**environ)):
            run('. run_g4c_model.cray cold 2007010100 2007011000 '
                '2007011000 48 {name}'.format(**environ))


@env_options
@task
def prepare_expdir(environ, **kwargs):
    print(fc.yellow('Preparing expdir'))
    run('mkdir -p {expdir}/exec'.format(**environ))
    # FIXME: hack to get remote path. Seems put can't handle shell env vars in
    # remote_path
    remote_path = str(run('echo {expdir}'.format(**environ))).split('\n')[-1]
    put('{exppath}/*'.format(**environ), remote_path, mirror_local_mode=True)


@env_options
@task
def prepare_workdir(environ, **kwargs):
    print(fc.yellow('Preparing workdir'))
    run('mkdir -p {workdir}'.format(**environ))
    run('cp -R {workdir_template}/* {workdir}'.format(**environ))
    run('touch {workdir}/time_stamp.restart'.format(**environ))


@env_options
@task
def clean_model_compilation(environ, **kwargs):
    print(fc.yellow("Cleaning code dir"))
    with shell_env(environ):
        with cd(os.path.join((environ['expdir'], 'exec'))):
            with prefix('source {envconf}'.format(**environ)):
                run('make -f {makeconf} clean'.format(**environ))


@env_options
@task
def compile_model(environ, **kwargs):
    print(fc.yellow("Compiling code"))
    with shell_env(environ):
        with prefix('source {envconf}'.format(**environ)):
            with cd('{expdir}/exec'.format(**environ)):
                #TODO: generate RUNTM and substitute
                run('make -f {makeconf}'.format(**environ))
            with cd(environ['comb_exe']):
                run('make -f {comb_src}/Make_combine'.format(**environ))
            with cd(environ['posgrib_src']):
                fix_posgrib_makefile(environ)
                run('mkdir -p {PATH2}'.format(**environ))
                run('make cray'.format(**environ))

@env_options
@task
def fix_posgrib_makefile(environ, **kwargs):
    run("sed -i.bak -r -e 's/^PATH2/#PATH2/g' Makefile")

@env_options
@task
def check_code(environ, **kwargs):
    print(fc.yellow("Checking code"))
    if environ['clean_checkout']:
        run('rm -rf {code_dir}'.format(**environ))
    if not exists(environ['code_dir']):
        print(fc.yellow("Creating new repository"))
        run('hg clone {code_repo} {code_dir}'.format(**environ))
    with cd(environ['code_dir']):
        print(fc.yellow("Updating existing repository"))
        run('hg pull')
        run('hg update {code_branch}'.format(**environ))
#        run('hg update -r{revision}'.format(**env))


@env_options
@task
def link_agcm_inputs(environ, **kwargs):
    run('mkdir -p {rootexp}/AGCM-1.0/model'.format(**environ))
    if not exists('{rootexp}/AGCM-1.0/model/datain'.format(**environ)):
        print(fc.yellow("Linking AGCM input data"))
        if not exists('{agcm_inputs}'.format(**environ)):
            run('mkdir -p {agcm_inputs}'.format(**environ))
            run('cp -R $ARCHIVE_OCEAN/database/AGCM-1.0/model/datain {agcm_inputs}'.format(**environ))
        run('ln -s {agcm_inputs} '
            '{rootexp}/AGCM-1.0/model/datain'.format(**environ))

@env_options
@task
def link_agcm_pos_inputs(environ, **kwargs):
    run('mkdir -p {rootexp}/AGCM-1.0/pos'.format(**environ))
    if not exists('{rootexp}/AGCM-1.0/pos/datain'.format(**environ)):
        print(fc.yellow("Linking AGCM post-processing input data"))
        if not exists('{agcm_pos_inputs}'.format(**environ)):
            run('mkdir -p {agcm_pos_inputs}'.format(**environ))
            run('cp -R $ARCHIVE_OCEAN/database/AGCM-1.0/pos/datain {agcm_pos_inputs}'.format(**environ))
        run('ln -s {agcm_pos_inputs} '
            '{rootexp}/AGCM-1.0/pos/datain'.format(**environ))
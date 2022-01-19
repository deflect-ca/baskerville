import os

from baskerville import src_dir


def get_parent_dir(directory):
    return os.path.dirname(directory)


def git_clone(git_url, branch=None, path=None):
    if not git_url.startswith('git@'):
        raise RuntimeError('git_clone supports only git@ urls.')

    if path:
        os.chdir(path)

    ssh_path = os.path.join(os.path.dirname(src_dir), 'ssh')
    ssh_key = None
    if os.path.exists(ssh_path):
        known_hosts = os.path.join(ssh_path, 'known_hosts')
        if not os.path.exists('/root/.ssh') and os.path.exists(known_hosts):
            os.system(f'mkdir /root/.ssh; cp {known_hosts} /root/.ssh')

        origin_ssh_key = os.path.join(ssh_path, 'id_rsa')
        if os.path.exists(origin_ssh_key):
            # copy the key in order to chmod to work
            ssh_key = os.path.join(os.path.dirname(src_dir), 'id_rsa')
            os.system(f'cp {origin_ssh_key} {ssh_key}')
            os.chmod(ssh_key, 0o600)

    if ssh_key:
        ssh = f'-c core.sshCommand="ssh -i {ssh_key}"'
    else:
        ssh = ''

    if branch:
        os.system(f'git {ssh} clone --single-branch --branch {branch} {git_url}')
    else:
        os.system(f'git {ssh} clone {git_url}')

    return os.path.join(os.getcwd(), os.path.splitext(os.path.basename(git_url))[0])


if __name__ == '__main__':
    print(git_clone('git@github.com:equalitie/baskerville_config.git'))

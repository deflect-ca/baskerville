# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import os
import sys

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

py_license = """# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
"""

html_license = """<a rel="license" href="http://creativecommons.org/licenses/by/4.0/">
<img alt="Creative Commons Licence" style="border-width:0"
src="https://i.creativecommons.org/l/by/4.0/80x15.png" /></a><br />
This work is copyright (c) 2020, eQualit.ie inc., and is licensed under a
<a rel="license" href="http://creativecommons.org/licenses/by/4.0/">
Creative Commons Attribution 4.0 International License</a>."""


def add_license_to_docs(dir_):
    """
    Add license / copyright in the html docs
    """
    import os

    for root, dirs, files in os.walk(dir_):
        path = root.split(os.sep)
        for file in files:
            if '.html' in file:
                full_path = os.path.join(root, file)
                with open(full_path) as html_f:
                    parsed_html = BeautifulSoup(html_f.read())
                    if parsed_html:
                        footer = parsed_html.body.find('footer')
                        footer.append(
                            BeautifulSoup(html_license, 'html.parser')
                        )
                with open(full_path, 'wb') as html_out_f:
                    html_out_f.write(parsed_html.prettify("utf-8"))
                    print('Wrote...', full_path)

            print(len(path) * '---', file)
            print(os.path.join(root, file))


def check_license(dir_, ext=('py', 'md', 'html'), exclude_dirs=('alembic',)):
    """
    Checks if all files with the defined extensions have the copyright license
    """
    licence_check = '(c) 2020, eQualit.ie inc.'
    errors = []
    for root, dirs, files in os.walk(dir_):

        if [d for d in exclude_dirs if d in root]:
            continue
        for f_ in files:
            f_ext = f_.rsplit('.', 1)[-1]
            if f_ext in ext:
                f_path = os.path.join(root, f_)
                with open(f_path) as curr_f:
                    contents = curr_f.read()
                    if len(contents) > 1 and licence_check not in contents:
                        errors.append(f_path)
                        print('License NOT found in: ', f_path)
    return errors


if __name__ == '__main__':
    add_license_to_docs('./../../../docs')
    errors = check_license('./../../../')
    if errors:
        print(errors)
        sys.exit(1)

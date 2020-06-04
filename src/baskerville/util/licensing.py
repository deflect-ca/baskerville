# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

html_license = """
<a rel="license" href="http://creativecommons.org/licenses/by/4.0/">
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


if __name__ == '__main__':
    add_license_to_docs('./../../docs')

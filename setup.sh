#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')   # Always put this in Bourne shell scripts

#Install necessary software
sudo apt install \
                    git \
                    sqlite3 \
                    perltidy \
                    cpanminus \
                    carton \
                    libspatialite-dev \
                    spatialite-bin \
                    libsqlite3-mod-spatialite \
                    gdal-bin
                     
#Install the libraries in our cpanfile locally
carton install

#Setup hooks to run perltidy on git commit
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
find . -maxdepth 1 -type f -name '*.pl' -or -name '*.pm' | \
    xargs -I{} -P0 sh -c 'perltidy -b -noll {}'
EOF

chmod +x .git/hooks/pre-commit

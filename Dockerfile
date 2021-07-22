FROM amd64/python:3.7-slim-buster as build-env
RUN apt-get upgrade && apt-get update

WORKDIR /home/idsuser/performance_hotspots

COPY ./ph/*.py ./ph/
COPY ./models/.placeholder ./models/
COPY ./data/*.test_dataset.csv ./data/

COPY requirements.txt ./
RUN pip install --no-cache-dir --trusted-host pypi.python.org -r requirements.txt

FROM scratch
COPY --from=build-env /home/idsuser/performance_hotspots /home/idsuser/performance_hotspots

COPY --from=build-env /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2
COPY --from=build-env /usr/local/bin/python3 /usr/local/bin/python3
COPY --from=build-env /usr/local/bin/python3.7 /usr/local/bin/python3.7

COPY --from=build-env /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/libgcc_s.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/libbz2.so.1.0.4 /lib/x86_64-linux-gnu/libbz2.so.1.0.4
COPY --from=build-env /lib/x86_64-linux-gnu/libbz2.so.1.0 /lib/x86_64-linux-gnu/libbz2.so.1.0
COPY --from=build-env /lib/x86_64-linux-gnu/libbz2.so.1 /lib/x86_64-linux-gnu/libbz2.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/libz.so.1.2.11 /lib/x86_64-linux-gnu/libz.so.1.2.11
COPY --from=build-env /lib/x86_64-linux-gnu/libz.so.1 /lib/x86_64-linux-gnu/libz.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/librt-2.28.so /lib/x86_64-linux-gnu/librt-2.28.so
COPY --from=build-env /lib/x86_64-linux-gnu/librt.so.1 /lib/x86_64-linux-gnu/librt.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/libcrypt.so.1 /lib/x86_64-linux-gnu/libcrypt.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/libpthread.so.0 /lib/x86_64-linux-gnu/libpthread.so.0
COPY --from=build-env /lib/x86_64-linux-gnu/libdl.so.2 /lib/x86_64-linux-gnu/libdl.so.2
COPY --from=build-env /lib/x86_64-linux-gnu/libutil.so.1 /lib/x86_64-linux-gnu/libutil.so.1
COPY --from=build-env /lib/x86_64-linux-gnu/libm.so.6 /lib/x86_64-linux-gnu/libm.so.6
COPY --from=build-env /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6
COPY --from=build-env /lib/x86_64-linux-gnu/liblzma.so.5 /lib/x86_64-linux-gnu/liblzma.so.5

COPY --from=build-env /usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.25 /usr/lib/x86_64-linux-gnu/libstdc++.so.6.0.25
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libstdc++.so.6 /usr/lib/x86_64-linux-gnu/libstdc++.so.6
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libssl.so.1.1 /usr/lib/x86_64-linux-gnu/libssl.so.1.1
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1 /usr/lib/x86_64-linux-gnu/libcrypto.so.1.1
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libffi.so.6 /usr/lib/x86_64-linux-gnu/libffi.so.6
COPY --from=build-env /usr/lib/x86_64-linux-gnu/libffi.so.6.0.4 /usr/lib/x86_64-linux-gnu/libffi.so.6.0.4

COPY --from=build-env /etc/ /etc
COPY --from=build-env /usr/lib/ssl /usr/lib/ssl
COPY --from=build-env /usr/local/lib /usr/local/lib

COPY --from=build-env /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libc.so.6
COPY --from=build-env /lib/x86_64-linux-gnu/libresolv.so.2 /lib/x86_64-linux-gnu/libresolv.so.2
COPY --from=build-env /lib/x86_64-linux-gnu/libresolv-2.28.so /lib/x86_64-linux-gnu/libresolv-2.28.so
COPY --from=build-env /lib/x86_64-linux-gnu/libnss_dns-2.28.so /lib/x86_64-linux-gnu/libnss_dns-2.28.so
COPY --from=build-env /lib/x86_64-linux-gnu/libnss_dns.so.2 /lib/x86_64-linux-gnu/libnss_dns.so.2
COPY --from=build-env /lib/x86_64-linux-gnu/libnss_files-2.28.so /lib/x86_64-linux-gnu/libnss_files-2.28.so
COPY --from=build-env /lib/x86_64-linux-gnu/libnss_files.so.2 /lib/x86_64-linux-gnu/libnss_files.so.2

COPY --from=build-env /var /var

ENV PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

ENV PYTHONPATH=/home/idsuser/performance_hotspots/
ENV ES_CA_CERT=placeholder
ENV ELASTIC_USER=placeholder
ENV ELASTIC_PASSWORD=placeholder

WORKDIR /home/idsuser/performance_hotspots/
ENTRYPOINT ["python3", "-m", "ph"]

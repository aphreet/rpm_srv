from opensuse:latest as builder

RUN zypper install -y wget tar gcc

RUN wget -O /opt/rust.tgz https://static.rust-lang.org/dist/rust-1.17.0-x86_64-unknown-linux-gnu.tar.gz \
	 && tar -C /opt -zxvf /opt/rust.tgz \
	 && /opt/rust-1.17.0-x86_64-unknown-linux-gnu/install.sh \
	 && rm -rf /opt/rust*

RUN mkdir -p /opt/rpm_srv

COPY . /opt/rpm_srv

RUN cd /opt/rpm_srv \
	&& cargo build --release \
	&& cp /opt/rpm_srv/target/release/rpm_srv /opt/srv \
	&& rm -rf /opt/rpm_srv

from opensuse:latest

RUN zypper install -y createrepo_c

COPY --from=builder /opt/srv /opt/srv

EXPOSE 8080

ENV RUST_LOG=rpm_srv=debug

ENTRYPOINT ["/opt/srv", "--rpm_root", "/var/rpms/"]

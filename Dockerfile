FROM public.ecr.aws/sam/build-python3.13 AS base
FROM timoreymann/deterministic-zip:latest AS zip-tool
FROM base
COPY --from=zip-tool /deterministic-zip /usr/local/bin/deterministic-zip
WORKDIR /app
COPY package_lambda_helper.sh /app/
RUN chmod +x /app/package_lambda_helper.sh
ENTRYPOINT ["/app/package_lambda_helper.sh"]
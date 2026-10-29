# Debian-based, not Alpine: Confluent.Kafka ships native librdkafka binaries and
# the musl variant needs extra packages that the glibc image avoids.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY service-dotnet/service-dotnet.csproj service-dotnet/
RUN dotnet restore service-dotnet/service-dotnet.csproj

COPY . .
RUN dotnet publish service-dotnet/service-dotnet.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0

WORKDIR /app

COPY --from=build /app ./

EXPOSE 3003

ENTRYPOINT ["dotnet", "service-dotnet.dll"]

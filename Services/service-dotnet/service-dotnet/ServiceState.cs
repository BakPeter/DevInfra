using System.Text.Json.Serialization;

namespace service_dotnet
{
    public record ServiceState
    {
        [JsonPropertyName("publishedAt")]
        public DateTimeOffset PublishedAt { get; init; }

        [JsonPropertyName("originService")]
        public string OriginService { get; init; } = string.Empty;

        [JsonPropertyName("healthState")]
        public string HealthState { get; init; } = string.Empty;

        [JsonPropertyName("checks")]
        public IDictionary<string, string> Checks { get; init; } = new Dictionary<string, string>();
    }
}

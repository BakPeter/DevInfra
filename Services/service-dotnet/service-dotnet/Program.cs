using Serilog;
using Serilog.Events;

var builder = WebApplication.CreateBuilder(args);

var seqServerUrl = builder.Configuration["SEQ_SERVER_URL"];

if (string.IsNullOrWhiteSpace(seqServerUrl))
{
    seqServerUrl = "http://localhost:5341";
}

builder.Host.UseSerilog((context, services, loggerConfiguration) => loggerConfiguration
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.Seq(seqServerUrl));

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddHostedService<service_dotnet.ServiceStatePublisher>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseSerilogRequestLogging();

// Only redirect when an HTTPS port is actually configured; the container runs plain HTTP.
if (app.Environment.IsDevelopment())
{
    app.UseHttpsRedirection();
}

app.UseAuthorization();

app.MapControllers();

app.Run();

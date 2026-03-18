using Charisma.Migration.Introspection.Push;
using Charisma.Schema;

namespace Charisma.Migration.Tests;

public sealed class SchemaServicesTests
{
    [Fact]
    public async Task SchemaPullService_DelegatesToIntrospectorAndWriter()
    {
        var schema = BuildSchema();
        var introspector = new FakeIntrospector(schema);
        var writer = new FakeWriter();
        var service = new SchemaPullService(introspector, writer);

        var result = await service.PullAsync("schema.charisma", overwrite: true);

        Assert.True(result.Written);
        Assert.True(introspector.Called);
        Assert.True(writer.Called);
        Assert.Equal("schema.charisma", writer.LastPath);
        Assert.True(writer.LastOverwrite);
    }

    [Fact]
    public async Task SchemaPushService_DelegatesToPusher()
    {
        var schema = BuildSchema();
        var pusher = new FakePusher();
        var service = new SchemaPushService(pusher);

        await service.PushAsync(schema);

        Assert.True(pusher.Called);
        Assert.Same(schema, pusher.LastSchema);
    }

    [Fact]
    public async Task SchemaPushService_NullSchema_Throws()
    {
        var pusher = new FakePusher();
        var service = new SchemaPushService(pusher);

        await Assert.ThrowsAsync<ArgumentNullException>(() => service.PushAsync(null!));
    }

    private static CharismaSchema BuildSchema()
    {
        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["Robot"] = new ModelDefinition("Robot", new FieldDefinition[]
                {
                    new ScalarFieldDefinition("id", "Id", false, false, new[] { "@id" }, isId: true)
                }, Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }

    private sealed class FakeIntrospector : ISchemaIntrospector
    {
        private readonly CharismaSchema _schema;

        public FakeIntrospector(CharismaSchema schema)
        {
            _schema = schema;
        }

        public bool Called { get; private set; }

        public Task<CharismaSchema> IntrospectAsync(CancellationToken cancellationToken = default)
        {
            Called = true;
            return Task.FromResult(_schema);
        }
    }

    private sealed class FakeWriter : ISchemaWriter
    {
        public bool Called { get; private set; }
        public string? LastPath { get; private set; }
        public bool LastOverwrite { get; private set; }

        public Task<SchemaWriteResult> WriteAsync(CharismaSchema schema, string schemaPath, bool overwrite, CancellationToken cancellationToken = default)
        {
            Called = true;
            LastPath = schemaPath;
            LastOverwrite = overwrite;
            return Task.FromResult(new SchemaWriteResult(Written: true, Skipped: false, Reason: null));
        }
    }

    private sealed class FakePusher : ISchemaPusher
    {
        public bool Called { get; private set; }
        public CharismaSchema? LastSchema { get; private set; }

        public Task PushAsync(CharismaSchema schema, CancellationToken cancellationToken = default)
        {
            Called = true;
            LastSchema = schema;
            return Task.CompletedTask;
        }
    }
}

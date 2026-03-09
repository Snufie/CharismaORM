using System;
using System.Linq;
using Xunit;
using Charisma.Parser;
using Charisma.Schema;

namespace Charisma.Parser.Tests
{
  public sealed class ParserTests
  {
    private readonly ISchemaParser _parser = new RoslynSchemaParser();

    // ----------------------------------------------------------------------
    //  POSITIVE TEST — full practice schema
    // ----------------------------------------------------------------------

    [Fact]
    public void Parse_PracticeSchema_Succeeds()
    {
      string schema = """
            // practice.dbschema - Prisma-like DSL for Charisma (derived from provided DDL)

            enum LaadStatusEnum {
              LADEN
              VOL
              LOW
              IDLE
            }

            enum SensorTypeEnum {
              TEMPERATUUR
              DRUK
              VOCHT
              LICHT
              ANDERS
            }

            enum SensorStatusEnum {
              ACTIEF
              INACTIEF
              DEFECT
            }

            enum CommandoTypeEnum {
              START
              STOP
              MOVE
              SHUTDOWN
              CUSTOM
            }

            enum CommandoStatusEnum {
              VERZONDEN
              UITGEVOERD
              MISLUKT
              IN_AFWACHTING
            }

            enum GebruikerRolEnum {
              ADMIN
              OPERATOR
              VIEWER
            }

            model Locatie {
              LocatieID    UUID   @id @db.Uuid
              Naam         String
              Adres        String?
              AantalRobots Int?
              created_at   DateTime @default(now()) @db.Timestamptz
              updated_at   DateTime @default(now()) @db.Timestamptz

              Robots       Robot[]
            }

            model Gebruiker {
              UserID      UUID           @id @db.Uuid
              Naam        String
              Email       String         @unique
              Rol         GebruikerRolEnum
              created_at  DateTime       @default(now()) @db.Timestamptz
              updated_at  DateTime       @default(now()) @db.Timestamptz

              CommandosVerzonden Commando[]  
            }

            model Robot {
              RobotID      UUID    @id @db.Uuid
              LocatieID    UUID?   @db.Uuid
              Naam         String
              Model        String?
              Actief       Boolean   @default(true)
              BatterijPercentage Int?
              LaadStatus   LaadStatusEnum
              created_at   DateTime  @default(now()) @db.Timestamptz
              updated_at   DateTime  @default(now()) @db.Timestamptz

              Locatie      Locatie?  @relation(fk: (Robot.LocatieID), pk: (Locatie.LocatieID))
              Sensors      Sensor[]
              Commandos    Commando[]
            }

            model Sensor {
              SensorID    UUID          @id @db.Uuid
              RobotID     UUID          @db.Uuid
              Type        SensorTypeEnum
              MeetEenheid String?
              Status      SensorStatusEnum
              created_at  DateTime        @default(now()) @db.Timestamptz
              updated_at  DateTime        @default(now()) @db.Timestamptz

              Robot       Robot           @relation(fk: (Sensor.RobotID), pk: (Robot.RobotID))
              Metingen    Meting[]
            }

            model Meting {
              MetingID   UUID   @id @db.Uuid
              SensorID   UUID   @db.Uuid
              Waarde     Decimal
              created_at DateTime @default(now()) @db.Timestamptz
              updated_at DateTime @default(now()) @db.Timestamptz

              Sensor     Sensor   @relation(fk: (Meting.SensorID), pk: (Sensor.SensorID))
            }

            model Commando {
              CommandoID          UUID           @id @db.Uuid
              RobotID             UUID           @db.Uuid
              VerzondenDoorUserID UUID?          @db.Uuid
              Type                CommandoTypeEnum
              Payload             Json?
              Status              CommandoStatusEnum
              FoutCode            String?
              Response            Json?
              created_at          DateTime         @default(now()) @db.Timestamptz
              updated_at          DateTime         @default(now()) @db.Timestamptz

              Robot        Robot      @relation(fk: (Commando.RobotID), pk: (Robot.RobotID))
              VerzondenDoor Gebruiker? @relation("Gebruiker_Commandos", fk: (Commando.VerzondenDoorUserID), pk: (Gebruiker.UserID))
            }
            """;
      Console.WriteLine("Parsing practice schema...");
      var result = _parser.Parse(schema);
      Console.WriteLine(result);

      Assert.NotNull(result);
      Assert.True(result.Models.ContainsKey("Robot"));
      Assert.True(result.Models.ContainsKey("Gebruiker"));
      Assert.True(result.Enums.ContainsKey("LaadStatusEnum"));
      Assert.True(result.Enums.ContainsKey("CommandoStatusEnum"));

      // basic sanity: check Robot.Locatie relation exists
      var robot = result.Models["Robot"];
      var locField = robot.Fields.First(f => f.Name == "Locatie") as RelationFieldDefinition;
      Assert.NotNull(locField);
      Assert.Equal("Locatie", locField!.RawType);
      Assert.NotNull(locField.RelationInfo);
      Assert.Contains("LocatieID", locField.RelationInfo!.LocalFields);
    }

    // ----------------------------------------------------------------------
    // NEGATIVE TESTS
    // ----------------------------------------------------------------------

    [Fact]
    public void Parse_UnknownScalar_ThrowsAggregate()
    {
      string schema = """
            model A {
                id Int @id
                oops xyz
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("Unknown type 'xyz'"));
    }

    [Fact]
    public void Parse_MissingPrimaryKey_ThrowsAggregate()
    {
      string schema = """
            model A {
                value Int
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("does not declare a primary key"));
    }

    [Fact]
    public void Parse_UnknownRelationModel_Throws()
    {
      string schema = """
            model A {
              id Int @id
              b  B @relation(fk: (A.id), pk: (B.id))
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("Unknown type 'B'"));
    }

    [Fact]
    public void Parse_UnknownRelationField_Throws()
    {
      string schema = """
            model A {
              id Int @id
              x  A2
            }

            model A2 {
              id Int @id
              wrongField A @relation(fk: (A2.notReal), pk: (A.id))
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("unknown field 'notReal'"));
    }

    [Fact]
    public void Parse_DuplicateModels_Throws()
    {
      string schema = """
            model A {
                id Int @id
            }

            model A {
                id Int @id
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("Duplicate model 'A'"));
    }

    [Fact]
    public void Parse_InvalidFkSyntax_Throws()
    {
      string schema = """
            model A {
              id Int @id
              r  A @relation(fk: invalid, pk: (A.id))
            }
            """;

      var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
      Assert.Contains(ex.Errors, e => e.Message.Contains("Invalid relation endpoint token"));
    }
  }
}

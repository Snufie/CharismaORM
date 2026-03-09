// using System;
// using System.IO;
// using System.Linq;
// using Charisma.Generator;
// using Charisma.Parser;
// using Charisma.Generated;
// using Charisma.Client;
// using Charisma.Runtime;
// using System.Text.Json;
// using Charisma.Generated.Models;

// namespace Charisma.Generator.Tests;

// public class ClientTest
// {
//     [Fact]
//     public void CharismaClient_Creation_Works()
//     {
//         CharismaClientOptions options = new()
//         {
//             ConnectionString = "Host=localhost;Database=Robotnik;Username=postgres;Password=postgres",
//             Provider = ProviderOptions.PostgreSQL,
//             RootNamespace = "Charisma.Generated",
//             OutputDirectory = "./Generated"
//         };

//         CharismaClient client = new(options);
//         client.Commando.CreateAsync(new()
//         {
//             Data = new()
//             {
//                 Response = JsonDocument.Parse("{\"message\":\"Hello, World!\"}").RootElement,
//                 Robot = new()
//                 {
//                     Connect = new()
//                     {
//                         RobotID = new Guid("11111111-1111-1111-1111-111111111111"),
//                     }
//                 }
//             }

//         });
//         client.TransactionAsync(action: async tx =>
//         {
//             Robot? robot = await tx.Robot.FindUniqueAsync(new()
//             {
//                 Where = new()
//                 {
//                     RobotID = new Guid("11111111-1111-1111-1111-111111111111"),
//                 }
//             });
//             if (robot is null)
//             {
//                 throw new InvalidOperationException("Robot not found");
//             }
//             await tx.Robot.FindByIdAsync(robot.RobotID);
//             await tx.Robot.DeleteAsync(new()
//             {
//                 Where = new()
//                 {
//                     RobotID = robot.RobotID,
//                 }
//             });
//             tx.FailAndRollback();
//         });

//         Assert.NotNull(client);
//     }

//     [Fact]
//     public void CharismaClient_NullOptions_ThrowsException()
//     {
//         Assert.Throws<ArgumentNullException>(() => new CharismaClient(null!));
//     }
// }


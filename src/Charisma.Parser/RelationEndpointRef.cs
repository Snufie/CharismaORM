using System;

namespace Charisma.Parser
{
    /// <summary>
    /// Small typed representation of a relation endpoint extracted from tokens like fk(User.id) or pk(Post.authorId).
    /// Parser extracts these into RelationEndpointRef for use by validator/generator.
    /// </summary>
    public sealed class RelationEndpointRef
    {
        public string Model { get; }
        public string Field { get; }

        /// <summary>
        /// Creates a relation endpoint reference with model and field names.
        /// </summary>
        /// <param name="model">Owning model name.</param>
        /// <param name="field">Field name on the model.</param>
        public RelationEndpointRef(string model, string field)
        {
            Model = model ?? throw new ArgumentNullException(nameof(model));
            Field = field ?? throw new ArgumentNullException(nameof(field));
        }

        /// <summary>
        /// Parses a token like "Model.Field" into a relation endpoint reference.
        /// </summary>
        /// <param name="token">Token in the form Model.Field (whitespace tolerated around parts).</param>
        /// <returns>Parsed <see cref="RelationEndpointRef"/>.</returns>
        /// <exception cref="FormatException">Thrown when the token does not match Model.Field.</exception>
        public static RelationEndpointRef Parse(string token)
        {
            // Accept tokens like "User.id" or "User . id" (trim whitespace)
            if (string.IsNullOrWhiteSpace(token)) throw new FormatException("Empty relation endpoint token");

            var trimmed = token.Trim();
            // allow both "Model.field" or quoted forms not supported — basic grammar
            var parts = trimmed.Split(new[] { '.' }, 2);
            if (parts.Length != 2) throw new FormatException($"Invalid relation endpoint token '{token}'. Expected format Model.Field");
            var model = parts[0].Trim();
            var field = parts[1].Trim();
            if (string.IsNullOrEmpty(model) || string.IsNullOrEmpty(field)) throw new FormatException($"Invalid relation endpoint token '{token}'.");
            return new RelationEndpointRef(model, field);
        }
    }
}

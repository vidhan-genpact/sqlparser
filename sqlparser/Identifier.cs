namespace sqlparser
{
    class TableIdentifier
    {
        public string Alias { get;set;}
        public string Name{ get;set; }

        public TableIdentifier(string alias, string name)
        {
            Alias = alias;
            Name = name;
        }

        public TableIdentifier()
        {
            Alias = string.Empty;
            Name = string.Empty;
        }
    }
}

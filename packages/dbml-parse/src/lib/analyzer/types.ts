export enum ElementKind {
  Table = 'table',
  Enum = 'enum',
  Ref = 'ref',
  Note = 'note',
  Project = 'project',
  Indexes = 'indexes',
  TableGroup = 'tablegroup',
  TablePartial = 'tablepartial',
  Check = 'checks',
  Transform = 'transform',
}

export enum SettingName {
  Color = 'color',
  HeaderColor = 'headercolor',
  Note = 'note',

  PK = 'pk',
  PrimaryKey = 'primary key',
  Unique = 'unique',
  Ref = 'ref',
  NotNull = 'not null',
  Null = 'null',
  Increment = 'increment',
  Default = 'default',
  Name = 'name',
  Type = 'type',
  Check = 'check',

  Update = 'update',
  Delete = 'delete',

  // Transform-specific settings
  As = 'as',
  Agg = 'agg',
  Window = 'window',
  PartitionBy = 'partition_by',
  OrderBy = 'order_by',
  Frame = 'frame',
  Expr = 'expr',
}

CREATE TABLE [dbo].[RSVRTableErrorHistory] (
    [Flat File Source Error Output Column] VARCHAR (MAX) NULL,
    [ErrorCode]                            INT           NULL,
    [ErrorColumn]                          INT           NULL,
    [FlatFileName]                         VARCHAR (200) NULL,
    [DataTransferID]                       INT           NULL,
    [loaddate]                             DATETIME      DEFAULT (getdate()) NULL,
    [RSVRBusinessDate]                     DATETIME      NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];


GO
GRANT INSERT
    ON OBJECT::[dbo].[RSVRTableErrorHistory] TO dbo
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[RSVRTableErrorHistory] TO dbo
    AS [dbo];


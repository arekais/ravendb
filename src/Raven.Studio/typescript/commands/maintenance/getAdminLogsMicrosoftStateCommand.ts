import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import Dictionary = System.Collections.Generic.Dictionary;

interface GetAdminLogsMicrosoftStateCommandResult {
    IsActive: boolean;
    Loggers: Dictionary<string, string>;
}

class getAdminLogsMicrosoftStateCommand extends commandBase {
    
    execute(): JQueryPromise<GetAdminLogsMicrosoftStateCommandResult> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftState;
        
        return this.query<GetAdminLogsMicrosoftStateCommandResult>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get Microsoft logs state", response.responseText, response.statusText)) 
    }
}

export = getAdminLogsMicrosoftStateCommand;
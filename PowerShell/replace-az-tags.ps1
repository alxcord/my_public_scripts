# Substitui tags de todos os recursos de um RG por um novo valor
# Por Alex Cordeiro
# .\ApplyTagToResources.ps1 -ResourceGroupName "MeuGrupoDeRecursos" -TagName "MinhaTag" -TagContent "ConteúdoDaTag"
#.\replace-az-tags.ps1  -TagName "APLICACAO" -TagContent "GPATIO" -ResourceGroupName "RG-GESTAODEPATIO-APP-VCBR"

param (
    [string]$SubscriptionId,
    [string]$ResourceGroupName,
    [string]$TagName,
    [string]$TagContent
)

# 7cf2bdc1-d780-41d5-9d32-2f81017514ec é a Votorantim Cimentos

# Import the necessary modules
Import-Module Az.Resources

# Set the Azure subscription
Connect-AzAccount 
Select-AzSubscription -SubscriptionId $SubscriptionId

# Get all resources in the resource group
$resources = Get-AzResource -ResourceGroupName $ResourceGroupName

# Initialize the progress bar
$progress = 0
$totalResources = $resources.Count

# Loop through each resource
foreach ($resource in $resources) {
    # Get the current tags for the resource
    $tags = $resource.Tags

    # If the resource has no tags, create an empty hashtable
    if ($tags -eq $null) {
        $tags = @{}
    }

    # Update the tags for the resource
    Write-Output "Verificando/alterando: $($resource.Name)"
	
	
	# Check if the tag is already correct
    if ($tags[$TagName] -ne $TagContent) {

        # Add or update the tag
        $tags[$TagName] = $TagContent

        # Update the tags for the resource
		Set-AzResource -ResourceId $resource.ResourceId -Tag $tags -Force 

        # Display a message with the name of the modified resource
        Write-Output "Recurso modificado: $($resource.Name)"
    }
	
	$progress++
	Write-Progress -Activity "Aplicando tag aos recursos" -Status "$progress de $totalResources concluidos" -PercentComplete (($progress / $totalResources) * 100)
}

